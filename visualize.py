from networkx.drawing.nx_pydot import write_dot
from . import task
import json


def to_dot(graph, path):
    write_dot(graph, path)
