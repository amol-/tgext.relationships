from tg import config
from tg import hooks
from tg.configuration import milestones

import logging
log = logging.getLogger('tgext.relationships')


# This is the entry point of your extension, will be called
# both when the user plugs the extension manually or through tgext.pluggable
# What you write here has the same effect as writing it into app_cfg.py
# So it is possible to plug other extensions you depend on.
def plugme(configurator, options=None):
    if options is None:
        options = {}

    log.info('Setting up tgext.relationships extension...')
    milestones.config_ready.register(SetupExtension(configurator))

    # This is required to be compatible with the
    # tgext.pluggable interface
    return dict(appid='tgext.relationships')


# Most of your extension initialization should probably happen here,
# where it's granted that .ini configuration file has already been loaded
# in tg.config but you can still register hooks or other milestones.
class SetupExtension(object):
    def __init__(self, configurator):
        self.configurator = configurator

    def __call__(self):
        log.info('>>> Public files path is %s' % config['paths']['static_files'])
        hooks.register('startup', self.on_startup)

        def echo_wrapper_factory(handler, config):
            def echo_wrapper(controller, environ, context):
                log.info('Serving: %s' % context.request.path)
                return handler(controller, environ, context)
            return echo_wrapper

        # Application Wrappers are much like easier WSGI Middleware
        # that get a TurboGears context and return a Response object.
        self.configurator.register_wrapper(echo_wrapper_factory)

    def on_startup(self):
        log.info('+ Application Running!')


    
