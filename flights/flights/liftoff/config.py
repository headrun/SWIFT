class Config(object):
    DEBUG = False
    TESTING = False
    # SQLALCHEMY_DATABASE_URI = "mysql+pymysql://headrun:e7rQZZKD@bh4@localhost/innoviti_hb"
    SQLALCHEMY_DATABASE_URI = "mysql+mysqldb://root:root@localhost/Searching_mca"
    SECRET_KEY = 'gfgsdjfghjkfgksdgfksgdkjgdskjfgsdkjgdkjgjd'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

# class ProductionConfig(Config):
#     DATABASE_URI = 'mysql://user@localhost/foo'

# class DevelopmentConfig(Config):
#     DEBUG = True

# class TestingConfig(Config):
#     TESTING = True
