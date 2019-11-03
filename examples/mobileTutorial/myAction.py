"""Triggered actions when a mobile agent is under the coverage of a IoT device (edge/sensor)"""

import logging


class GenericAction:
    # service_coverage
    #   key   => street node network
    #   value => id. module SW

    def __init__(self, sim):  # sim is an instance of CORE.py
        self.sim = sim

    def action(self, mobile_agent):
        None


class CustomAction(GenericAction):
    def __init__(self, *args, **kwargs):
        super(CustomAction, self).__init__(*args, **kwargs)
        self.plates = {}
        self.fees = {}

    # mandatory function
    def action(self, ma):  # mobile_entity
        # print "ACTION"
        # print ma
        # print ma.next_time
        # print ma.get_current_position()
        # print "-"*10
        logging.info(" Performing Action from VEHICLE: %i in: %i " % (ma.id, ma.get_current_position()))

        if ma.get_current_position() in list(self.sim.service_coverage.keys()):  # sim is an instance of CORE.py
            if ma.plate in self.plates:
                self.fees[ma.plate] = {"arrive": self.plates[ma.plate], "end": self.sim.env.now}
            else:
                self.plates[ma.plate] = self.sim.env.now
