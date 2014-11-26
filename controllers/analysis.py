__author__ = 'dex'


def index():
    Analysis.series_ids.readable = True
    Analysis.platform_ids.readable = True
    Analysis.sample_ids.readable = True
    Analysis.series_count.readable = True
    Analysis.platform_count.readable = True
    Analysis.sample_count.readable = True

    grid = SQLFORM.grid(Analysis,
                        fields=[Analysis.analysis_name,
                                Analysis.description,
                                Analysis.case_query,
                                Analysis.control_query,
                                Analysis.modifier_query,
                                Analysis.series_count,
                                Analysis.platform_count,
                                Analysis.sample_count],
                        orderby=~Analysis.id,
                        search_widget=None,
                        searchable=None,
                        create=None,
                        # buttons_placement='left',
                        links=[lambda row: A("Results", _href=URL('results',
                                                                  vars=dict(
                                                                      analysis_id=row.id)))],
                        formname='results'
    )
    num_incomplete = db(Scheduler_Task.status <> "COMPLETED").count()
    queue = A("%s %s in queue" % (
        num_incomplete, "analysis" if num_incomplete == 1 else "analyses") if num_incomplete else "",
              _href=URL('queue'),
              _target="blank")
    return dict(add=A(BUTTON("New Analysis"), _href=URL("add"), vars=request.get_vars),
                # form=search_widget(),
                queue=queue,
                grid=grid
    )


def queue():
    grid = SQLFORM.grid(Scheduler_Task.status <> "COMPLETED",
                        fields=[Scheduler_Task.vars, Scheduler_Task.status, Scheduler_Task.start_time],
                        orderby=~Scheduler_Task.id,
                        create=None,
                        search_widget=None,
                        searchable=None,
                        formname='scheduler',
    )
    return dict(grid=grid)


def results():
    analysis_id = request.vars.analysis_id or redirect('index')
    query = Balanced_Meta.analysis_id == analysis_id
    # row = db(Analysis[analysis_id]).select()
    form_vars = dict(analysis_name=Analysis[analysis_id].analysis_name,
                     description=Analysis[analysis_id].description,
                     case_query=Analysis[analysis_id].case_query,
                     modifier_query=Analysis[analysis_id].modifier_query,
                     control_query=Analysis[analysis_id].control_query)

    return dict(form=BEAUTIFY(form_vars),
                analyze=A(BUTTON("Re-Analyze"), _href=URL("analyze", vars=form_vars)),
                grid=SQLFORM.grid(query,
                                  create=False,
                                  editable=False,
                                  searchable=False,
                                  deletable=False,
                                  search_widget=False,
                                  user_signature=None,
                                  buttons_placement='left',
                )
    )


def add():
    form = SQLFORM.factory(Analysis, submit_button="Next")
    form.add_button("Cancel", URL('index', vars=request.get_vars))
    if form.process().accepted:
        session.form_vars = form.vars
        redirect(URL('submit', vars=request.get_vars))
    return dict(form=form)


def submit():
    form_vars = session.form_vars
    for var in form_vars:
        Analysis[var].default = form_vars[var]
        Analysis[var].writable = False
    form = SQLFORM.factory(Analysis, submit_button="Analyze")
    form.add_button("Cancel", URL('index', vars=request.get_vars))

    if form.validate():
        redirect(URL("analyze", vars=form_vars))

    df = get_analysis_df(form_vars.case_query,
                         form_vars.control_query,
                         form_vars.modifier_query)

    stats = BEAUTIFY(dict(case_count=len(df.query('sample_class == 1').index),
                          control_count=len(df.query('sample_class == 0').index),
                          error_count=len(df.query('sample_class == -1').index)))

    return dict(stats=stats,
                form=form)


@auth.requires_login()
def analyze():
    pvars = dict(analysis_name=request.vars.analysis_name,
                 description=request.vars.description,
                 case_query=request.vars.case_query,
                 control_query=request.vars.control_query,
                 modifier_query=request.vars.modifier_query)
    go(pvars,
       # debug=True
    )
    # task_analyze(**pvars)
    redirect(URL('index'))


def go(pvars, debug=False):
    if debug:
        task_analyze(debug=pvars['analysis_name'], **pvars)
    else:
        # from datetime import timedelta as timed

        scheduler.queue_task('task_analyze',
                             pvars=dict(analysis_name=request.vars.analysis_name,
                                        description=request.vars.description,
                                        case_query=request.vars.case_query,
                                        control_query=request.vars.control_query,
                                        modifier_query=request.vars.modifier_query),
                             timeout=3600,
                             # start_time=request.utcnow - timed(hours=8),  # correct 8 hrs for CA time,
                             immediate=True
        )
        session.flash = T("Analysis '%s' queued..." % request.vars.analysis_name)
    return True
