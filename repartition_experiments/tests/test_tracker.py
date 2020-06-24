from ..algorithms.tracker import Tracker
from ..algorithms.utils import Volume


def test_add_volume():
    tracker = Tracker()
    tracker.add_volume(Volume(0, (5,5,5), (10,10,10)))
    
    assert len(tracker.i_dict.keys()) == 1
    assert len(tracker.i_dict[(5,10)]) == 1
    assert len(tracker.i_dict[(5,10)][(5,10)]) == 1
    assert list(tracker.i_dict[(5,10)][(5,10)].keys())[0] == (5,10)


def test_is_complete():
    tracker = Tracker()
    tracker.add_volume(Volume(0, (0,0,0), (10,10,5)))
    assert not tracker.is_complete((10,10,10))

    tracker.add_volume(Volume(0, (0,0,5), (10,10,10)))
    assert tracker.is_complete((10,10,10))