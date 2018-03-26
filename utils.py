import os


def get_arg(env, default):
    """
    Get environment variable value or default, if not set
    :param env: The environment variable to read
    :type env: string
    :param default: Default value to return if not set
    :type default: string
    :return: Environment variable value of default if not set
    :rtype string
    """
    return os.getenv(env) if os.getenv(env, '') is not '' else default
