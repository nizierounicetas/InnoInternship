import configparser


class ConfigurationManager:
    """ Class with single static method to read credentials from config file. """
    @classmethod
    def get_configuration(cls, config_file: str, env: str) -> configparser.SectionProxy:
        """
        Reads configuration json_data from config file.
            Parameters:
                config_file (str): Path to configuration file.
                env (str): Environment.
            Returns:
                configparser.SectionProxy: Object of configuration for particular environment.
        """
        db_config = configparser.ConfigParser()
        db_config.read(config_file)
        return db_config[env]
