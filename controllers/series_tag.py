def index():
    return dict(form=search_widget(),
                refresh=A(BUTTON("Refresh"), _href=URL('refresh', 'index')),
                grid=get_grid())
