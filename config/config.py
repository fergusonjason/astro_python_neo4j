import configparser

config = configparser.ConfigParser()
config.read('app.properties')

if config == None:
    print("No config found")
    quit()

URI = config['Database']['uri']
USER = config['Database']['user']
PASSWORD = config['Database']['password']