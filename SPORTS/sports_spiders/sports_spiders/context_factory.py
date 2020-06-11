from OpenSSL import SSL
from scrapy.core.downloader.contextfactory import ScrapyClientContextFactory
from twisted.internet.ssl import ClientContextFactory
from twisted.internet._sslverify import ClientTLSOptions

class CustomClientContextFactory(ScrapyClientContextFactory):
    def getContext(self, hostname=None, port=None):
        ctx = ScrapyClientContextFactory.getContext(self)
        ctx.set_options(SSL.OP_ALL)
        if hostname:
            ClientTLSOptions(hostname, ctx)
        return ctx

class MyClientContextFactory(ScrapyClientContextFactory):
    def __init__(self):
        self.method = SSL.SSLv23_METHOD
