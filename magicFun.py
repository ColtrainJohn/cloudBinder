import pandas as pd
import config


def chooseWHOlineage(line):
    if line[5].find('Omicron') > -1 or line[6].find('Omicron') > -1:
        return 'Omicron (B.1.1.529 + BA.*)'
    elif line[5].find('BA.') > -1 or line[6].find('BA.') > -1:
        return 'Omicron (B.1.1.529 + BA.*)'
    elif line[5].find('B.1.1.529') > -1 or line[6].find('B.1.1.529') > -1:
        return 'Omicron (B.1.1.529 + BA.*)'
    elif line[5].find('Delta') > -1 or line[5].find('AY.') > -1 or line[6].find('Delta') > -1:
        return 'Delta (B.1.617.2 + AY.*)'
    elif line[5].find('B.1.617.2') > -1 or line[6].find('B.1.617.2') > -1:
        return 'Delta (B.1.617.2 + AY.*)'
    elif line[5].find('Alpha') > -1 or line[5].find('Q.') > -1 or line[6].find('Alpha') > -1:
        return 'Alpha (B.1.1.7 + Q.*)'
    elif line[5].find('Beta') > -1 or line[6].find('Beta') > -1:
        return 'Beta (B.1.351)'
    elif line[5].find('Gamma') > -1 or line[6].find('Gamma') > -1:
        return 'Gamma (P.1)'
    return 'Не относится к "Variants of Concern"'


def lineageFromWHOchoice(line):
    if line[7].find('Omicron') > -1:
        if line[5].find('BA.') > -1:
            return line[5]
        else:
            return 'B.1.1.529'
    if line[7].find('Delta') > -1:
        if line[5].find('AY.') > -1:
            return line[5]
        else:
            return 'B.1.617.2'
    if line[7].find('Alpha') > -1:
        if line[5].find('Q.') > -1:
            return line[5]
        else:
            return 'B.1.1.7'
    if line[7].find('Beta') > -1:
            return 'B.1.351'
    if line[7].find('Gamma') > -1:
            return 'P.1'
    if line[7].find("Variants of Concern") > -1:
            if line[5] in ['None', 'B', 'B.1', 'B.1.1', 'Unassigned']:
                return 'B*'
            return line[5]


def cropOmicron(x):
    if not x.startswith('B.'):
        x = '.'.join(x.split('.')[:2])
    return x


def dateSplitter(date):
    try:
        date = pd.to_datetime(date)
        year = date.year
        month = monthNames[date.month]
        week = date.week
        if week > 6 and month == '01 Январь':
            week = 1
        week = '0' + str(week) if len(str(week)) == 1 else str(week)
        week = f'{week} неделя'
    except:
        year = 'Неизвестно'
        month = 'Неизвестно'
        week = 'Неизвестно'
    return year, month, week


def agrTab(tab):
    tab[7] = tab.apply(lambda variant: chooseWHOlineage(variant), axis=1)
    tab[8] = tab.apply(lambda lineage: lineageFromWHOchoice(lineage), axis=1)
    tab[8] = tab[8].apply(lambda ba_x: cropOmicron(ba_x))
    tab[[9, 10, 11]] = pd.DataFrame([dateSplitter(date) for date in tab[1].values.tolist()])
    tab = tab.loc[(tab[1] != 'None') & (tab[4].isin(config.regions))]
    return tab




monthNames = {
    1: '01 Январь',
    2: '02 Февраль',
    3: '03 Март',
    4: '04 Апрель',
    5: '05 Май',
    6: '06 Июнь',
    7: '07 Июль',
    8: '08 Август',
    9: '09 Сентябрь',
    10: '10 Октябрь',
    11: '11 Ноябрь',
    12: '12 Декабрь',
    }


def shapeTab(data, basename=False):
    tab = agrTab(data)
    if basename == 'table1':
        return tab
    elif basename == 'table2':
        return table2agg(tab)
    elif basename == 'table3':
        return tab[[7, 8]].value_counts().reset_index()
        

def table2agg(data):
    total = data[4].value_counts().reindex(config.regions, fill_value=0).reset_index()
    partsToConcat = []
    for who in data[7].drop_duplicates().values:
        variantTable = data.loc[data[7] == who][4].value_counts().reindex(config.regions, fill_value=0).reset_index()
        variantTable['selector'] = who
        partsToConcat.append(variantTable)
    out = pd.concat(partsToConcat, axis=0)
    out = out.merge(total, on='index')
    out['percentage'] = out['4_x'] / out['4_y']
    out['percentage'] = out['percentage'].map(lambda x: round(x * 100, 2))
    out = out[['selector', 'index', '4_x', 'percentage', '4_y']]
    out.columns = ['whoLine', 'Region', 'NseqLine', 'Percentage', 'NseqAll']
    return out.astype('str')

def table2agg(data):
    totals = data[[4, 9]].value_counts().reindex(pd.MultiIndex.from_product([config.regions, [2020, 2021, 2022]]), fill_value=0).reset_index()
    totals.columns = ['Region', 'Year', 'Total']
    partsToConcat = []
    for who in data[7].drop_duplicates().values:
        variantTable = data.loc[data[7] == who][[4, 9]].value_counts().reindex(pd.MultiIndex.from_product([config.regions, [2020,2021,2022]]), fill_value=0).reset_index()
        variantTable['selector'] = who
        partsToConcat.append(variantTable)
    out = pd.concat(partsToConcat, axis=0)
    out.columns = ['Region', 'Year', 'Ncounts', 'selector']
    out = out.merge(totals)
    out['percentage'] = out['Ncounts'] / out['Total']
    out['percentage'] = out['percentage'].map(lambda x: round(x * 100, 2))
    return out[['Year', 'Region', 'selector', 'Ncounts', 'percentage', 'Total']].astype('str')
