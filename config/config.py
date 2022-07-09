import configparser
import os, sys, inspect

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


filename = os.path.join(os.path.dirname(__file__), "app.properties")
if not os.path.isfile(filename):
    print("config/app.properties not found")
    quit()

config = configparser.ConfigParser()
config.read(filename)

URI = config['Database']['uri']
USER = config['Database']['user']
PASSWORD = config['Database']['password']