import sys
import os
import socket
import logging
import bugsnag
from bugsnag.handlers import BugsnagHandler
import traceback
root_path = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger()

BASIC_FORMAT = f"%(levelname)s:{os.getenv('HOSTNAME')}:%(name)s:%(message)s"

if os.getenv("ENABLE_FILE_LOG") or os.name == 'nt':
    fh = logging.FileHandler("rss_errors.log")
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

logging.basicConfig(format=BASIC_FORMAT, stream=sys.stdout, level=logging.ERROR)


def attach_fields(record, options):
    if "metadata" not in options:
        options['metadata'] = {}

    options['metadata']['server'] = {
        'hostname': socket.gethostname()
    }

if os.getenv("BUGSNAG_APIKEY"):
    bugsnag.configure(api_key=os.getenv("BUGSNAG_APIKEY"), project_root=root_path)
    bugsnag_handler = BugsnagHandler()
    bugsnag_handler.add_callback(attach_fields)
    logger.addHandler(bugsnag_handler)
        
def notify(e):
    if os.getenv("BUGSNAG_APIKEY"):
        try:
            bugsnag.notify(e)
        except:
            pass
    else:
        traceback.print_exc()

class ScriptExit(Exception):pass
class SigKill(Exception):pass
class ChildProcDied(Exception):pass
class FlagKill(Exception):pass
class ArticleEmpty(Exception):pass
class MLException(Exception):pass
class PageError(Exception):pass

def sigkill(*args):
    hostname = socket.gethostname()
    args = [hostname] + list(args)
    notify(SigKill("SignalKill:"+":".join([str(x) for x in args])))
    
def flagkill(*args):
    hostname = socket.gethostname()
    args = [hostname] + list(args)
    notify(FlagKill("FlagKill:"+":".join([str(x) for x in args])))
    
def artempty(*args):
    hostname = socket.gethostname()
    args = [hostname] + list(args)
    notify(ArticleEmpty("ArticleEmpty:"+":".join([str(x) for x in args])))
    
def mlerr(*args):
    hostname = socket.gethostname()
    args = [hostname] + list(args)
    notify(ArticleEmpty("ArticleEmpty:"+":".join([str(x) for x in args])))

def pageerr(*args):
    hostname = socket.gethostname()
    args = [hostname] + list(args)
    notify(PageError("PageLoadError:"+":".join([str(x) for x in args])))
    
def condolense(*args):
    hostname = socket.gethostname()
    args = [hostname] + list(args)
    notify(ChildProcDied("Child Process Dead:"+":".join([str(x) for x in args])))
    