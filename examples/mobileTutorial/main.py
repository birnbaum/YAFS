import json
import logging.config
import os
import subprocess
import time

import trackanimation
from yafs.application import Application, Message
from yafs.core import Simulation
from yafs.coverage import CircleCoverage
from yafs.customMovement import MovementUpdate
from yafs.distribution import *
from yafs.placement import JSONPlacement
from yafs.population import JSONPopulation2
from yafs.selection import DeviceSpeedAwareRouting
from yafs.topology import Topology


def create_applications_from_json(data):
    applications = {}
    for app in data:
        a = Application(name=app["name"])
        modules = [{"None": {"Type": Application.TYPE_SOURCE}}]
        for module in app["module"]:
            modules.append({module["name"]: {"RAM": module["RAM"], "Type": Application.TYPE_MODULE}})
        a.set_modules(modules)

        ms = {}
        for message in app["message"]:
            # print "Creando mensaje: %s" %message["name"]
            ms[message["name"]] = Message(message["name"], message["s"], message["d"], instructions=message["instructions"], size=message["bytes"])
            if message["s"] == "None":
                a.add_source_message(ms[message["name"]])

        # print "Total mensajes creados %i" %len(ms.keys())
        for idx, message in enumerate(app["transmission"]):
            if "message_out" in list(message.keys()):
                a.add_service_module(message["module"], ms[message["message_in"]], ms[message["message_out"]])
            else:
                a.add_service_module(message["module"], ms[message["message_in"]])

        applications[app["name"]] = a

    return applications


def main(path, path_results, number_simulation_steps, tracks, topology, case, it, doExecutionVideo):
    """
    Prepares the rest of experiment configuration
    """

    """
    Mobile entities and Workload
    """
    # Declaring mobile fog entities according with NAME.gpx
    # a list of strings
    #
    mobile_fog_entities = {
        "6197472": {
            "connectionWith": [0, 5],  # ids of network entities (independent of its location)
            "node_attributes": {
                "IPT": 30006,
                "RAM": 4000
                # and whatever you want for your especfic strategies
            },
        },
        "20604255": {
            "connectionWith": None,  # it uses the coverage.connection_between_mobile_entities by default (dependent)
            "node_attributes": {
                "IPT": 30006,
                "RAM": 4000
                # and whatever you want for your especfic strategies
            },
        },
    }

    # mobile_fog_entities =["6197472"]

    """
    APPLICATION
    """
    dataApp = json.load(open(path + "appDefinition.json"))
    apps = create_applications_from_json(dataApp)

    """
    PLACEMENT algorithm
    """
    # In our model only initial cloud placements are enabled
    placementJson = json.load(open(path + "allocDefinition.json"))
    placement = JSONPlacement(name="Placement", json=placementJson)

    """
    SELECTOR and Deploying algorithm
    """
    selectorPath = DeviceSpeedAwareRouting()

    """
    SIMULATION ENGINE
    """
    s = Simulation(topology, default_results_path=path_results + "Results_%s_%i" % (case, it))

    """
    MOBILE - parametrization
    """
    s.load_user_tracks(tracks)  # TODO "user_tracks" has been removed from Simulation
    s.set_coverage_class(CircleCoverage, radius=5)  # radius in KM  #TODO This was removed from the core
    # s.set_coverage_class(Voronoi)
    s.set_mobile_fog_entities(mobile_fog_entities)  # TODO has been removed from Simulation

    # Expensive task
    # It generates a short video (mp4) with the movement of users in the coverage (without network update)
    # s.generate_animation(path_results+"animation_%s" % case)

    """
    Linking workload activity && Population phase
    """
    # Each user/mobile entity has an unique population politic
    wl = json.load(open(path + "workload.json"))  # workload behaviour
    pop = JSONPopulation2(name="Statical", json=wl, it=it)

    """
    Deploying application with specific distribution in the simulator
    # Penultimate phase
    """
    for aName in list(apps.keys()):
        # print "Deploying app: ",aName
        pop_app = JSONPopulation2(name="Statical_%s" % aName, json={}, it=it)
        data = []
        for element in pop.data["sources"]:
            if element["app"] == aName:
                data.append(element)
        pop_app.data["sources"] = data

        s.deploy_app(apps[aName], placement, pop_app, selectorPath)

    """
    Creating the custom monitor that manages the movement of mobile entities
    """
    stop_time = number_simulation_steps * time_in_each_step

    dStart = DeterministicDistributionStartPoint(0, time_in_each_step, name="Deterministic")
    evol = MovementUpdate(path_results, doExecutionVideo)
    s.deploy_monitor("Traces_localization_update", evol, dStart, **{"sim": s, "routing": selectorPath, "case": case, "stop_time": stop_time, "it": it})

    s.set_movement_control(evol)  # TODO has been removed from the core

    """
    RUNNING
    """
    logging.info(" Performing simulation: %s %i " % (case, it))
    s.run(stop_time, progress_bar=False, mobile_behaviour=True)

    """
    Storing results from customized strategies
    """
    # Getting some info
    s.print_debug_assignaments()

    # print "----"
    # entities = s.alloc_entities
    # src_entities,modules_entities = Counter(),Counter()
    # for k, v in entities.iteritems():
    #     src_entities[k]=0
    #     modules_entities[k]=0
    #     for service in v:
    #         if "None" in service:
    #             src_entities[k]+=1
    #         elif "_" in service:
    #             modules_entities[k]+=1 #[u'3#3_22', u'2#2_19']
    #
    #
    # nx.set_node_attributes(s.topology.G, values=src_entities,name="SRC")
    # nx.set_node_attributes(s.topology.G, values=modules_entities,name="MOD")
    #
    # nx.write_gexf(s.topology.G, pathResults + "/network_assignments_%s_%i_%i.gexf" % (case, stop_time, it))

    # controlServices = selectorPath.controlServices
    # f = open(pathResults + "/file_assignments_%s_%i_%i.pkl" % (case, stop_time, it), "wb")
    # pickle.dump(controlServices, f)
    # f.close()


def do_video_from_execution_snaps(output_file, png_names, framerate):
    cmdstring = (
        "ffmpeg",
        "-loglevel",
        "quiet",
        "-framerate",
        str(framerate),
        "-i",
        png_names,
        "-r",
        "25",
        "-s",
        "1280x960",
        "-pix_fmt",
        "yuv420p",
        output_file + ".mp4",
    )

    subprocess.call(cmdstring)


if __name__ == "__main__":

    # Logging can be avoided comment these three lines
    import logging.config

    wd_path = os.getcwd()
    logging.config.fileConfig(wd_path + "/logging.ini")
    #

    ##
    # STEP-0:
    # Initial parametrization of the experiment
    ##

    # As we perform the simulations in external server, we simplify the path value according with the WD_path
    print(wd_path)
    if "/home/uib/" in wd_path:
        experiment_path = "/home/uib/src/YAFS/src/examples/ConquestService/exp/"
    else:
        experiment_path = "exp/"
    print("Experiment Path ", experiment_path)
    #

    # Experiment variables
    nSimulations = 1
    number_simulation_steps = 100
    time_in_each_step = 1000

    datestamp = time.strftime("%Y%m%d")
    datestamp = "20190326"
    temporal_folder = experiment_path + "results_" + datestamp + "/"

    trajectories_path = experiment_path + "/trajectories/"

    try:
        os.makedirs(temporal_folder)
    except OSError:
        None

    ##
    # STEP-1:
    # Initializing of the common and static context of each simulation
    ##

    # 1.1 Mobile entities trough GPX traces
    # The track normalization is an expensive computational task. A cached file is generated in each temporal path
    if os.path.isfile(temporal_folder + "normalized_trajectories.csv"):
        input_directory = temporal_folder + "normalized_trajectories.csv"  #
        logging.info("Loading trajectories from (cached file): %s" % input_directory)
        tracks = trackanimation.read_track(input_directory)
    else:
        input_directory = trajectories_path  # can load csv files
        logging.info("Loading trajectories from (raw files): %s" % input_directory)
        tracks = trackanimation.read_track(input_directory)
        tracks = tracks.time_video_normalize(time=number_simulation_steps, framerate=1)  # framerate must be one
        tracks.export(temporal_folder + "normalized_trajectories")

    # 1.2 Network infrastructure
    # Endpoint entities must have three attributes: level(=0) and lat/lng coordinates
    t = Topology()
    dataNetwork = json.load(open(experiment_path + "networkDefinition.json"))
    # TODO This method got removed in order to simplify/remove the topology class. All graph construction should happen outside of the class.
    """
    self.G = nx.Graph()
        for edge in data["link"]:
            self.G.add_edge(edge["s"], edge["d"], BW=edge[self.LINK_BW], PR=edge[self.LINK_PR])

        dc = {str(x): {} for x in list(data["entity"][0].keys())}
        for ent in data["entity"]:
            for key in list(ent.keys()):
                dc[key][ent["id"]] = ent[key]
        for x in list(data["entity"][0].keys()):
            nx.set_node_attributes(self.G, values=dc[x], name=str(x))

        for node in data["entity"]:
            self.G.nodes[node["id"]] = node

        self._init_uptimes()
    """
    t.load_all_node_attr(dataNetwork)

    # Performing multiple simulations
    for i in range(nSimulations):
        random.seed(i)
        np.random.seed(i)
        logging.info("Running Mobility Case - %s" % experiment_path)
        start_time = time.time()

        main(
            path=experiment_path,
            path_results=temporal_folder,
            number_simulation_steps=number_simulation_steps,
            tracks=tracks,
            topology=t,
            case="one",
            doExecutionVideo=True,  # expensive task
            it=i,
        )

        print(("\n--- %s seconds ---" % (time.time() - start_time)))
        do_video_from_execution_snaps(temporal_folder + "animation_snaps", "snap_%05d.png", 10)

    print("Simulation Done!")
    # ffmpeg -r 2 -i snap_%05d.png -c:v libx264 -vf fps=1 -pix_fmt yuv420p out.mp4
# ffmpeg -c:v -framerate 10 -f image2pipe -i snap_%03d.png -r 25 -s 1280x960 -pix_fmt yuv420p video_test.mp4
# ffmpeg -r 1 -i snap_%05d.png -c:v libx264 -vf fps=1 -pix_fmt yuv420p out2.mp4
