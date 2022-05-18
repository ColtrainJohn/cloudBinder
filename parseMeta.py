import pandas as pd

# ВРЯД ЛИ ЭТО ДОЛЖНО БЫТЬ ТАК НАПИСАНО
def parseMetaJson(meta):
    seqId = meta['internal_number']
    
    try:
        region = meta['addresses'][0]['region']['name_ru']
        if region == 'Севастополь':
            region = 'г Севастополь'
    except:
        region = 'None'
    
    try:
        federal = meta['addresses'][0]['federalDistrict']['name_ru']
    except:
        federal = 'None'
    
    try:
        org = meta['depart']['depart_name']
    except:
        org = 'None'

    try:
        sampleDate = pd.to_datetime(meta['sample']['created_at']).date()
    except:
        sampleDate = 'None'
    
    try:
        seqDate = pd.to_datetime(meta['file']['created_at']).date()
    except:
        seqDate = 'None'
    
    try:
        collectionDate = pd.to_datetime(meta['formValue']['sample_pick_date']['value']).date()
    except:
        collectionDate = 'None'
    
    try:
        seqArea = meta['formValue']['seq_area']['value']
    except:
        seqArea = 'None'

    try:
        pangoNew = meta['pangolinResult']['lineage']
    except TypeError:
        pangoNew = 'None'
        
    try:
        parus = meta['result']['name']
    except TypeError:
        parus = 'None'


    out = [
        seqId,
        collectionDate,
        seqDate,
        federal,
        region, 
        pangoNew,
        parus,
        seqArea
        ]
    
    return out
