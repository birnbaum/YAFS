import numpy as np
import pandas as pd

from yafs.metrics import EventLog


# TODO Missing documentation
class Stats:
    def __init__(self, event_log: EventLog):
        self.messages = pd.DataFrame(event_log.message_log)
        self.transmission = pd.DataFrame(event_log.transmission_log)

    def count_messages(self):
        return len(self.messages)

    def bytes_transmitted(self):
        return self.transmission["size"].sum()

    def utilization(self, id_entity, total_time, from_time=0.0):
        if "time_service" not in self.messages.columns:  # cached
            self.messages["time_service"] = self.messages.time_out - self.messages.time_in
        values = self.messages.groupby("DES.dst").time_service.agg("sum")
        return values[id_entity] / total_time

    def compute_times_df(self):
        self.messages["time_latency"] = self.messages["time_reception"] - self.messages["time_emit"]
        self.messages["time_wait"] = self.messages["time_in"] - self.messages["time_reception"]  #
        self.messages["time_service"] = self.messages["time_out"] - self.messages["time_in"]
        self.messages["time_response"] = self.messages["time_out"] - self.messages["time_reception"]
        self.messages["time_total_response"] = self.messages["time_response"] + self.messages["time_latency"]

    def times(self, time, value="mean"):
        if "time_response" not in self.messages.columns:
            self.compute_times_df()
        return self.messages.groupby("message").agg({time: value})

    def average_loop_response(self, time_loops):
        """
        No hay chequeo de la existencia del loop: user responsability
        """
        if "time_response" not in self.messages.columns:
            self.compute_times_df()

        resp_msg = self.messages.groupby("message").agg({"time_total_response": ["mean", "count"]})  # Its not necessary to have "count"
        resp_msg.columns = ["_".join(col).strip() for col in resp_msg.columns.values]
        results = []

        for loop in time_loops:
            total = 0.0
            for msg in loop:
                try:
                    total += resp_msg[resp_msg.index == msg].time_total_response_mean[0]
                except IndexError:
                    total += 0

            results.append(total)

        return results

    def get_watt(self, totaltime, topology, by=EventLog.WATT_SERVICE):
        results = {}
        if by == EventLog.WATT_SERVICE:
            # Tiempo de actividad / runeo
            if "time_response" not in self.messages.columns:  # cached
                self.compute_times_df()

            nodes = self.messages.groupby("TOPO.dst").agg({"time_service": "sum"})
            for id_node in nodes.index:
                results[id_node] = {
                    "model": topology.G.nodes[id_node]["model"],
                    "type": topology.G.nodes[id_node]["type"],
                    "watt": nodes.loc[id_node].time_service * topology.G.nodes[id_node]["WATT"],
                }
        else:
            for node_key in topology.G.nodes:
                if not topology.G.nodes[node_key]["uptime"][1]:
                    end = totaltime
                start = topology.G.nodes[node_key]["uptime"][0]
                uptime = end - start  # TODO end may be undefined
                results[node_key] = {
                    "model": topology.G.nodes[node_key]["model"],
                    "type": topology.G.nodes[node_key]["type"],
                    "watt": uptime * topology.G.nodes[node_key]["WATT"],
                    "uptime": uptime,
                }

        return results

    def get_cost_cloud(self, topology):
        cost = 0.0
        nodeInfo = topology.G.nodes
        results = {}
        # Tiempo de actividad / runeo
        if "time_response" not in self.messages.columns:  # cached
            self._compute_times_df()

        nodes = self.messages.groupby("TOPO.dst").agg({"time_service": "sum"})

        for id_node in nodes.index:
            if nodeInfo[id_node]["type"] == Entity.ENTITY_CLOUD:
                results[id_node] = {
                    "model": nodeInfo[id_node]["model"],
                    "type": nodeInfo[id_node]["type"],
                    "watt": nodes.loc[id_node].time_service * nodeInfo[id_node]["WATT"],
                }
                cost += nodes.loc[id_node].time_service * nodeInfo[id_node]["COST"]
        return cost, results

    def print_report(self, total_time, topology, time_loops=None):
        print(("\tSimulation Time: %0.2f" % total_time))

        if time_loops is not None:
            print("\tApplication loops delays:")
            results = self.average_loop_response(time_loops)
            for i, loop in enumerate(time_loops):
                print(("\t\t%i - %s :\t %f" % (i, str(loop), results[i])))

        # print("\tEnergy Consumed (WATTS by UpTime):")
        # values = self.get_watt(total_time, topology, Metrics.WATT_UPTIME)
        # for node in values:
        #    print(("\t\t%i - %s :\t %.2f" % (node, values[node]["model"], values[node]["watt"])))

        # print("\tEnergy Consumed by Service (WATTS by Service Time):")
        # values = self.get_watt(total_time, topology, Metrics.WATT_SERVICE)
        # for node in values:
        #    print(("\t\t%i - %s :\t %.2f" % (node, values[node]["model"], values[node]["watt"])))

        # print("\tCost of execution in cloud:")
        # total, values = self.get_cost_cloud(topology)
        # print(("\t\t%.8f" % total))

        print("\tNetwork bytes transmitted:")
        print(("\t\t%.1f" % self.bytes_transmitted()))

        print("\t- Network saturation -")
        print("\t\tAverage waiting messages : %i" % self.average_messages_not_transmitted())
        print("\t\tPeak of waiting messages : %i" % self.peak_messages_not_transmitted())
        print("\t\tTOTAL messages not transmitted: %i" % self.messages_not_transmitted())

    def valueLoop(self, total_time, time_loops=None):  # TODO Improve this interface
        if time_loops is not None:
            results = self.average_loop_response(time_loops)
            for i, loop in enumerate(time_loops):
                return results[i]

    def average_messages_not_transmitted(self):
        return np.mean(self.transmission.buffer)

    def peak_messages_not_transmitted(self):
        return np.max(self.transmission.buffer)

    def messages_not_transmitted(self):
        return self.transmission.buffer[-1:]

    def get_df_modules(self):
        g = self.messages.groupby(["module", "DES.dst"]).agg({"service": ["mean", "sum", "count"]})
        return g.reset_index()

    def get_df_service_utilization(self, service, time):
        """Returns the utilization(%) of a specific module"""
        g = self.messages.groupby(["module", "DES.dst"]).agg({"service": ["mean", "sum", "count"]})
        g.reset_index(inplace=True)
        h = pd.DataFrame()
        h["module"] = g[g.module == service].module
        h["utilization"] = g[g.module == service]["service"]["sum"] * 100 / time
        return h
