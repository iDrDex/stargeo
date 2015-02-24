__author__ = 'dex'

def index():
    return dict(tree=IMG(_src=URL('static', 'tree_of_death.svg')),
                refresh = A(BUTTON("Redraw"), _href=URL('redraw'))
    )

def redraw():
    saveTree()
    redirect(URL('index'), client_side=True)
