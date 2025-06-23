from collections import defaultdict
from pipeline.models import Position

state: dict[str, Position] = defaultdict(Position)