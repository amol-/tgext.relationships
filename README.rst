About tgext.relationships
-------------------------

tgext.relationships is a TurboGears2 extension

Installing
-------------------------------

tgext.relationships can be installed from pypi::

    pip install tgext.relationships

should just work for most of the users.

Enabling
-------------------------------

To enable tgext.relationships put inside your application
``config/app_cfg.py`` the following::

    import tgext.relationships
    tgext.relationships.plugme(base_config)

or you can use ``tgext.pluggable`` when available::

    from tgext.pluggable import plug
    plug(base_config, 'tgext.relationships')
