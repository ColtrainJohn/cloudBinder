urlSeqMeta = 'https://genome.crie.ru/list/page-items'
urlSeqCount = 'https://genome.crie.ru/list/count'
pageSize = 1000
headers = {'Authorization': "Bearer bdcf09ac54d663cf154550f26b66229d34af2f8dc52827b1f9568eb05f9e82c7"}
params = {'page_size' : pageSize, 'page_id': 0}
DBparam = {
            "host" : "rc1b-9y7za9y2712fpiyf.mdb.yandexcloud.net",
            "user" : "sinev",
            "password" : "55sXt25ckd",
            "port" : 6432,
            "dbname" : "datalens"
        }


columnSourceNames = [
    "internal_number",
    #"valid",
    #"formValue.author.value",
    #"formValue.tech.value",
    #"formValue.seq_area.value",
    #"formValue.sample_pick_place.value",
    "formValue.sample_pick_date.value",
    #"formValue.patient_gender.value",
    #"formValue.patient.age.value",
    #"formValue.issue.value",
    #"formValue.vaccine.value",
    #"formValue.patient_social_status.value",
    #"formValue.sick_form.value",
    #"formValue.hospitalization.value",
    #"formValue.contact_persons.value",
    #"formValue.infection_source.value",
    #"sample.depart_name",
    #"depart.depart_name",
    "pangolinResult.lineage",
    "parusResult.foundType.name",
    #"addresses",
    "file.created_at",
    ]

geoLoc = [
    "federalDistrict.name_ru",
    "region.name_ru"
        ]

colToCol = {
        "internal_number" : "vgarusId",
        "formValue.sample_pick_date.value" : "pickDate",
        "file.created_at" : "loadDate",
        "federalDistrict.name_ru" : "federal",
        "region.name_ru" : "region",
        "pangolinResult.lineage" : "pango",
        "parusResult.foundType.name" : "parus"
        }

