# class for saving results of parsing xml files

class Result:
    def __init__(self, title, production, manufacturer, moto_type, category, engine, transmission):
        self.title = title
        self.production = production
        self.manufacturer = manufacturer
        self.moto_type = moto_type
        self.category = category
        self.engine = engine
        self.transmission = transmission
