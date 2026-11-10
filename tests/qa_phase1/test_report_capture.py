"""Windows readers may temporarily deny replacement of the JSON result file."""
import errno
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parent))


@pytest.mark.qa_case('N-WIN-001','Atomic artifact writes tolerate bounded Windows sharing violations',
    priority='P1',expected='Transient sharing violations recover; permanent filesystem errors remain visible',
    technique='filesystem/fault-injection')
@pytest.mark.parametrize('transient',[True,False])
def test_result_capture_file_lock(monkeypatch,tmp_path,transient):
    import campaign
    original=Path.replace
    calls=[]
    with monkeypatch.context() as patch:
        patch.setattr(campaign,'ART',tmp_path)
        patch.setattr(campaign.time,'sleep',lambda seconds:None)
        def replace(path,target):
            calls.append(1)
            if transient and len(calls)<=2:raise PermissionError(errno.EACCES,'Sharing violation')
            if not transient:raise OSError(errno.ENOSPC,'No space left')
            return original(path,target)
        patch.setattr(Path,'replace',replace)
        if transient:
            campaign.save('result.json',{'status':'PASS'})
            assert json.loads((tmp_path/'result.json').read_text(encoding='utf-8'))=={'status':'PASS'}
            assert len(calls)==3
        else:
            with pytest.raises(OSError) as error:campaign.save('result.json',{})
            assert error.value.errno==errno.ENOSPC
            assert len(calls)==1
