import pytest
from .safety import SafetyError


def pytest_addoption(parser):
    group=parser.getgroup('cloud-e2e')
    group.addoption('--cloud-scope',default=None,help='Explicit approved scope JSON; normally supplied by safe runner')
    group.addoption('--cloud-mode',default='off',choices=['off','e2e','reliability','performance','all'])
    group.addoption('--allow-destructive',action='store_true',help='Owned, isolated test resources only')
    group.addoption('--cloud-case-id',action='append',default=[],help='Optional explicit case IDs for independent regression rechecks')


def pytest_collection_modifyitems(config,items):
    for item in items:
        marker=item.get_closest_marker('cloud_case')
        if not marker:continue
        selected=config.getoption('--cloud-case-id')
        if selected and marker.kwargs['id'] not in selected:
            item.add_marker(pytest.mark.skip(reason='NOT RUN: different explicitly selected cloud case'))
            continue
        if not config.getoption('--cloud-scope') or config.getoption('--cloud-mode')=='off':
            item.add_marker(pytest.mark.skip(reason='BLOCKED: cloud tests are opt-in; no approved isolated stack'))
        elif marker.kwargs.get('destructive') and not config.getoption('--allow-destructive'):
            item.add_marker(pytest.mark.skip(reason='BLOCKED: owned fault injection not explicitly enabled'))
        else:
            mode=config.getoption('--cloud-mode');kind=marker.kwargs['kind']
            if mode!='all' and kind not in ('integration',mode):
                item.add_marker(pytest.mark.skip(reason='NOT RUN: different selected cloud mode'))
    # Dependencies and shared query watermark make the declared sequence intentional.
    # Every case still uses its own run-scoped track/user/chart keys.
    items.sort(key=lambda item:(item.get_closest_marker('cloud_case').kwargs['order'] if item.get_closest_marker('cloud_case') else -1,item.nodeid))


@pytest.fixture
def stack(request):
    harness=getattr(request.config,'cloud_harness',None)
    if harness is None:pytest.skip('BLOCKED: use scripts/run-cloud-tests.py with an owned test stack')
    marker=request.node.get_closest_marker('cloud_case')
    harness.current=marker.kwargs['id']
    try:harness.guard(force=True)
    except SafetyError as exc:pytest.skip('BLOCKED: safety gate: '+str(exc))
    yield harness


@pytest.fixture
def dataset(stack):
    from .data import Data
    return Data(stack)
