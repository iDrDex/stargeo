def index():
    return dict(form=search_widget(),
                grid=get_grid())

def test():
    query = Series_View_Results.search_id == 6
    return dict(grid=SQLFORM.grid(query))
