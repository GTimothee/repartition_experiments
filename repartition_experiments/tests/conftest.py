import pytest, logging, os, tempfile

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session", autouse=True)
def temporary_directory(): 
    """ Create a temporary directory to run all the tests into.
    """
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        yield
        os.chdir(old_cwd)