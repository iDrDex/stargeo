# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# ########################################################################
# # Customize your APP title, subtitle and menus here
# ########################################################################
# response.logo = A(B('web',SPAN(2),'py'),XML('&trade;&nbsp;'),
# _class="brand",_href="http://www.web2py.com/")
# response.title = request.application.replace('_',' ').title()
# response.subtitle = ''

response.title = "STAR"
# response.subtitle = "The Search Tag & Analyze Resource for collaborative annotation and interpretation of disease from open digital samples."
response.subtitle = "Redefining the meaning of disease... Together!"
response.logo = DIV(
    DIV(A(I(_class="fa fa-star"),
          B("GEO"),
          _href=URL('default', 'index'))
        , _class="brand"),

    A(B(session.query or ""),
      _href=URL('default',
                'index'),
      _class="brand"))

# # read more at http://dev.w3.org/html5/markup/meta.name.html
response.meta.author = 'Dexter Hadley <dexter@stanford.edu>'
response.meta.keywords = 'web2py, python, framework, meta-analysis, microarray, public data, crowdsource, gene signature, big data, translational genomics'
response.meta.generator = 'Search Tag Analyze Resource'

# # your http://google.com/analytics id
response.google_analytics_id = "UA-42964640-3"


# ########################################################################
# # this is the main application menu add/remove items as required
# ########################################################################

response.menu = [
    (SPAN(T('Home'), _class="highlighted" if request.controller == 'default' else ""),
     request.controller == 'default',
     URL('default', 'index', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
    (SPAN(T('Series'), _class="highlighted" if request.controller == 'series' else ""),
     request.controller == 'series',
     URL('series', 'index', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
    (SPAN(T('Samples'), _class="highlighted" if request.controller in ('sample', 'tag', 'annotate') else ""),
     request.controller in ('sample', 'tag', 'annotate'),
     URL('sample', 'index', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
    (SPAN(T('Annotations'), _class="highlighted" if request.controller == 'sample_tag' else ""),
     request.controller == 'sample_tag',
     URL('sample_tag', 'index', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
    (SPAN(T('Summary'), _class="highlighted" if request.controller == 'series_tag' else ""),
     request.controller == 'series_tag',
     URL('series_tag', 'index', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
    (SPAN(T('Analysis'), _class="highlighted" if request.controller == 'analysis' else ""),
     request.controller == 'analysis',
     URL('analysis', 'index', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
    (SPAN(T('Manage'), _class="highlighted" if request.controller.startswith('manage') else ""),
     request.controller.startswith('manage'),
     None,
     [(T('Tags'),
       request.controller.startswith('manage'),
       URL('manage', 'tag', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
      (T('Series Tags'),
       request.controller == 'manage',
       URL('manage', 'series_tag', vars=request.vars.keywords and dict(keywords=request.vars.keywords))),
     ]),
]

DEVELOPMENT_MENU = False

# ########################################################################
# # provide shortcuts for development. remove in production
# ########################################################################


def _():
    # shortcuts
    app = request.application
    ctr = request.controller
    # useful links to internal and external resources
    response.menu += [
        (SPAN('web2py', _class='highlighted'), False, 'http://web2py.com', [
            (T('My Sites'), False, URL('admin', 'default', 'site')),
            (T('This App'), False, URL('admin', 'default', 'design/%s' % app), [
                (T('Controller'), False,
                 URL(
                     'admin', 'default', 'edit/%s/controllers/%s.py' % (app, ctr))),
                (T('View'), False,
                 URL(
                     'admin', 'default', 'edit/%s/views/%s' % (app, response.view))),
                (T('Layout'), False,
                 URL(
                     'admin', 'default', 'edit/%s/views/layout.html' % app)),
                (T('Stylesheet'), False,
                 URL(
                     'admin', 'default', 'edit/%s/static/css/web2py.css' % app)),
                (T('DB Model'), False,
                 URL(
                     'admin', 'default', 'edit/%s/models/db.py' % app)),
                (T('Menu Model'), False,
                 URL(
                     'admin', 'default', 'edit/%s/models/menu.py' % app)),
                (T('Database'), False, URL(app, 'appadmin', 'index')),
                (T('Errors'), False, URL(
                    'admin', 'default', 'errors/' + app)),
                (T('About'), False, URL(
                    'admin', 'default', 'about/' + app)),
            ]),
            ('web2py.com', False, 'http://www.web2py.com', [
                (T('Download'), False,
                 'http://www.web2py.com/examples/default/download'),
                (T('Support'), False,
                 'http://www.web2py.com/examples/default/support'),
                (T('Demo'), False, 'http://web2py.com/demo_admin'),
                (T('Quick Examples'), False,
                 'http://web2py.com/examples/default/examples'),
                (T('FAQ'), False, 'http://web2py.com/AlterEgo'),
                (T('Videos'), False,
                 'http://www.web2py.com/examples/default/videos/'),
                (T('Free Applications'),
                 False, 'http://web2py.com/appliances'),
                (T('Plugins'), False, 'http://web2py.com/plugins'),
                (T('Layouts'), False, 'http://web2py.com/layouts'),
                (T('Recipes'), False, 'http://web2pyslices.com/'),
                (T('Semantic'), False, 'http://web2py.com/semantic'),
            ]),
            (T('Documentation'), False, 'http://www.web2py.com/book', [
                (T('Preface'), False,
                 'http://www.web2py.com/book/default/chapter/00'),
                (T('Introduction'), False,
                 'http://www.web2py.com/book/default/chapter/01'),
                (T('Python'), False,
                 'http://www.web2py.com/book/default/chapter/02'),
                (T('Overview'), False,
                 'http://www.web2py.com/book/default/chapter/03'),
                (T('The Core'), False,
                 'http://www.web2py.com/book/default/chapter/04'),
                (T('The Views'), False,
                 'http://www.web2py.com/book/default/chapter/05'),
                (T('Database'), False,
                 'http://www.web2py.com/book/default/chapter/06'),
                (T('Forms and Validators'), False,
                 'http://www.web2py.com/book/default/chapter/07'),
                (T('Email and SMS'), False,
                 'http://www.web2py.com/book/default/chapter/08'),
                (T('Access Control'), False,
                 'http://www.web2py.com/book/default/chapter/09'),
                (T('Services'), False,
                 'http://www.web2py.com/book/default/chapter/10'),
                (T('Ajax Recipes'), False,
                 'http://www.web2py.com/book/default/chapter/11'),
                (T('Components and Plugins'), False,
                 'http://www.web2py.com/book/default/chapter/12'),
                (T('Deployment Recipes'), False,
                 'http://www.web2py.com/book/default/chapter/13'),
                (T('Other Recipes'), False,
                 'http://www.web2py.com/book/default/chapter/14'),
                (T('Buy this book'), False,
                 'http://stores.lulu.com/web2py'),
            ]),
            (T('Community'), False, None, [
                (T('Groups'), False,
                 'http://www.web2py.com/examples/default/usergroups'),
                (T('Twitter'), False, 'http://twitter.com/web2py'),
                (T('Live Chat'), False,
                 'http://webchat.freenode.net/?channels=web2py'),
            ]),
            (T('Plugins'), False, None, [
                ('plugin_wiki', False,
                 'http://web2py.com/examples/default/download'),
                (T('Other Plugins'), False,
                 'http://web2py.com/plugins'),
                (T('Layout Plugins'),
                 False, 'http://web2py.com/layouts'),
            ])
        ]
        )]


if DEVELOPMENT_MENU: _()

if "auth" in locals(): auth.wikimenu() 
