# -*- coding: utf-8 -*-
import logging
import getpass
import sys
import os

class MyLog(object):
    def __init__(self, init_file=None):
        user = getpass.getuser()
        self.logger = logging.getLogger(user)
        self.level=logging.DEBUG #记录等级：debug < info< warning< error< critical
        self.logger.setLevel(self.level)
        if init_file == None:
            logFile = sys.argv[1]
        else:
            logFile = init_file
        logFile=logFile

        self.formatter = logging.Formatter('%(asctime)-12s %(levelname)-8s %(name)-10s %(message)-12s')

        self.filelogHand = logging.FileHandler(logFile, encoding="utf8")
        self.filelogHand.setFormatter(self.formatter)
        self.filelogHand.setLevel(logging.INFO)

        self.logHandSt = logging.StreamHandler()
        self.logHandSt.setLevel(self.level)
        self.logHandSt.setFormatter(self.formatter)


    def debug(self, msg):
        self.logger.handlers.clear()
        self.logger.addHandler(self.filelogHand)
        self.logger.addHandler(self.logHandSt)
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.handlers.clear()
        self.logger.addHandler(self.filelogHand)
        self.logger.addHandler(self.logHandSt)
        self.logger.info(msg)

    def warn(self, msg):
        self.logger.handlers.clear()
        self.logger.addHandler(self.filelogHand)
        self.logger.addHandler(self.logHandSt)
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.handlers.clear()
        self.logger.addHandler(self.filelogHand)
        self.logger.addHandler(self.logHandSt)
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.handlers.clear()
        self.logger.addHandler(self.filelogHand)
        self.logger.addHandler(self.logHandSt)
        self.logger.critical(msg)

if __name__=='__main__':
    mylog=MyLog('test.log')
    mylog.debug("I'm debug")
    mylog.info("I'm info")
    mylog.warn("I'm warning")
    mylog.error("I'm error")
    mylog.critical("I'm critical")
