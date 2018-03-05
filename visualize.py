from networkx.drawing.nx_pydot import write_dot
import task
import json


def to_dot(graph, path):
    write_dot(graph, path)
