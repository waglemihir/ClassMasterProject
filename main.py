from masterClass import MasterClass
from configs import get_cfg

if __name__ == '__main__':

    config = 'config/db_config.cfg'
    cfg = get_cfg(config)
    db_configs = cfg['db_config']

    masterclass = MasterClass(db_configs)
    masterclass.process_data()
    
