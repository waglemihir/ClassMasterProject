from configparser import ConfigParser


def get_cfg(config):
    cfg = ConfigParser()
    cfg.read(config)
    return cfg
