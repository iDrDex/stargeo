__author__ = 'dex'


def index():
    Scheduler_Task = db['scheduler_task']
    return dict(add=A("New Analysis", _href=URL("add"), vars=request.get_vars),
                # form=search_widget(),
                scheduler=SQLFORM.grid(Scheduler_Task.status <> "COMPLETED",
                                       fields=[Scheduler_Task.vars, Scheduler_Task.status, Scheduler_Task.start_time],
                                       orderby=~Scheduler_Task.id,
                                       create=None,
                                       search_widget=None
                ),
                results=SQLFORM.grid(Analysis,
                                  orderby=~Analysis.id,
                                  search_widget=None,
                                  create=None,
                                  # buttons_placement='left',
                                  links=[lambda row: A("Results", _href=URL('results',
                                                                            vars=dict(
                                                                                analysis_id=row.id)))],
                )
    )


def results():
    analysis_id = request.vars.analysis_id or redirect('index')
    query = Balanced_Meta.analysis_id == analysis_id
    # row = db(Analysis[analysis_id]).select()
    return dict(form=BEAUTIFY(Analysis[analysis_id]),
                grid=SQLFORM.grid(query))


def add():
    form = SQLFORM.factory(Analysis)
    form.add_button("Cancel", URL('index', vars=request.get_vars))
    if form.process().accepted:
        session.form_vars = form.vars
        redirect(URL('submit', vars=request.get_vars))
    return dict(form=form)


def submit():
    form_vars = session.form_vars
    df = get_analysis_df(form_vars.case_query, form_vars.control_query, form_vars.modifier_query)
    return dict(form_vars=form_vars,
                go=A("GO", _href=URL("analyze", vars=form_vars)),
                case_count=len(df.query('sample_class == 1').index),
                control_count=len(df.query('sample_class == 0').index),
                error_count=len(df.query('sample_class == -1').index),
    )


@auth.requires_login()
def analyze():
    pvars = dict(analysis_name=request.vars.analysis_name,
                 description=request.vars.description,
                 case_query=request.vars.case_query,
                 control_query=request.vars.control_query,
                 modifier_query=request.vars.modifier_query)
    # task_analyze(**pvars)
    scheduler.queue_task(
    task_analyze,
        pvars=pvars,
        timeout=3600,
        immediate=True,
    )


    # scheduler.queue_task('balanced_meta', pvars=dict(analysis_name=request.vars.analysis_name,
    # description=request.vars.description,
    #              case_query=request.vars.case_query,
    #              control_query=request.vars.control_query,
    #              modifier_query=request.vars.modifier_query)
    # )
    session.flash = T("Analysis '%s' queued..." % request.vars.analysis_name)
    redirect(URL('index'))
