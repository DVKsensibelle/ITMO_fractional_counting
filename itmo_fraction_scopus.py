
from pybliometrics.scopus import ScopusSearch
from pybliometrics.scopus import AbstractRetrieval
import pandas as pd

counter100=0 #для подсчета публикаций с 100+ соавторами

#задаем поисковую строку (с указанием года и т.д.) и айди организации, по которой ведется подсчет. если нужно несколько вариантов айди, можно дать в виде списка

querystring = '(AF-ID (60072485)) AND PUBYEAR  AFT 1980 AND PUBYEAR BEF 2023'

target_id = ['60072485']  #используем ID организации ITMO в scopus

#выгружаем через API публикации по нашему запросу и кладем в датафрейм

print ('Starting download...')
s = ScopusSearch(querystring, subscriber=True, verbose=True)
s_len=s.get_results_size() #число найденных публикаций
print ('found total results:',s_len)
df = pd.DataFrame(pd.DataFrame(s.results))
df.dropna(subset=['author_afids'],inplace=True) #убираем публикации без нужной нам информации об аффилиациях и авторах (обычно их нет)
print ('found results with required data:',s.get_results_size())

# основная функция для расчета доли

def fracount (xx):
    global counter100
    global s_len
    counter = 0  #это суммарная доля вуза
    au_ai=xx[15]
    affils=au_ai.split(';') #делаем список айди аффилиаций отдельных авторов из полученного поля со всеми аффилиациями авторов статьи
    n_auths = xx[12] #получаем общее число авторов для дальнейшего расчета
    eid=xx[0]
    print (xx.name, " of ", s_len)

    if n_auths != '100':  #если авторов менее 100, можно использовать готовую выгрузку по ScopusSearch (в ней режется всё сверх 100 авторов)

        for x in affils:
            affil=x.split('-')
            n_affils=len(affil) #получаем число аффилиаций у конкретного автора
            if any(item in target_id for item in affil):
                counter2=1/n_affils #считаем долю конкретного автора, имеющего нужную аффилиацию (делим на общее число аффилиаций у данного автора)
                counter=counter+counter2 #добавляем долю автора к суммарной доле вуза
            else:
                counter2=0
        n_auths3=int(n_auths)
        counter3=counter/n_auths3 #считаем финальную долю вуза по данной статье, деля суммарную долю вуза на число авторов
        return counter3

    else:  #для публикаций, где авторов больше 100, нужно выкачивать данные об аффилиации отдельно. число авторов, указанное в обычной выгрузке для них тоже некорректно
        ab = AbstractRetrieval(eid)
        df2 = pd.DataFrame(ab.authors)
        n_auths2 = len(df2.index)  #тут какая-то загвоздка. в ряде публикаций такой подсчет дает на одного автора меньше по сравнению с SciVal
        df['author_count'][xx.name]=str(n_auths2) #пишем в колонку с числом авторов правильную информацию вместо 100
        df2.dropna(subset=['affiliation'], inplace=True) #удаляем авторов без аффилиаций (в расчете общей доли они сохраняются)
        for x in df2['affiliation']:
            n_affils=len(x) #получаем число аффилиаций у конкретного автора
            if any(item in target_id for item in x):
                counter2=1/n_affils #считаем долю конкретного автора, имеющего нужную аффилиацию (делим на общее число аффилиаций у данного автора)
                counter=counter+counter2 #добавляем долю автора к суммарной доле вуза
            else:
                counter2=0
        counter3=counter/n_auths2 #считаем финальную долю вуза по данной статье, деля суммарную долю вуза на число авторов
        counter100=counter100+1
        return counter3

#рассчитываем доли по конкретным статьям

df['share'] = df.apply(fracount, axis=1)

# добавляем данные в общую таблицу и сохраняем ее в CSV и в экселе (публикации с большим числом соавторов могут отражаться в экселе некорректно)

print('total rows(publications) for export=',len(df.index))
print('more than 100 authors:',counter100)
df.to_csv(r'F:\DK2\scopuspubs_itmo_80_22.csv',sep='\t',mode='w')
print('exported to F:\DK2\scopuspubs_itmo_80_22.csv, tab-separated')
df.to_excel(r'F:\DK2\scopuspubs_itmo_80_22.xlsx')
print('exported to F:\DK2\scopuspubs_itmo_80_22.xlsx')