import numpy as np

from matplotlib.patches import Rectangle

from .point import Point

""" Recangle object from the matplotlib object, with more attributes and methods
"""


class CustomRectangle(Rectangle):
    def __init__(self, point, width, height, **kwargs):
        self.point = point  # lower corner
        self.width = width
        self.height = height
        self.corners = None     
        super(CustomRectangle, self).__init__((point.x, point.y), width, height, **kwargs)
        
    def data(self):
        return (self.point, self.width, self.height)
        
    def get_corners(self):
        """ Compute corners and return it
        """
        if self.corners is not None:
            return self.corners
        
        point = self.point
        width = self.width
        height = self.height 
        
        xx = [point.x, point.x + width]
        yy = [point.y, point.y + height]
        keys = [
            "bot_left",
            "bot_right",
            "top_left",
            "top_right"
        ]
        points = np.dstack(np.meshgrid(xx, yy)).reshape(-1, 2)
        vals = list(map(lambda pt: Point(pt[0], pt[1]), points))
        self.corners = dict(zip(keys, vals))
        return self.corners

    def setFig(self, figure):
        self.figure = figure
