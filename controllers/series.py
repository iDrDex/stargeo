def index():
    return dict(form=search_widget(),
                grid=get_grid())
