import os
import pandas as pd


data = []
for file in os.listdir('tmp/'):
        data.append(pd.read_csv('tmp/' + file, header=None))

data = pd.concat(data).iloc[:, :-4]
data.columns = ['busstopId', 'busstopNr', 'line', 'direction', 'long', 'lat', 'time']

data = data.sort_values('time')
extracted = []
for i in range(data.groupby(['line', 'busstopId', 'busstopNr']).size().max()):
    datai = data.loc[data.groupby(['line', 'busstopId', 'busstopNr']).cumcount() == i].copy()
    datai['iter'] = i
    extracted.append(datai)
extracted = pd.concat(extracted)

extracted.to_csv('lines/lines.csv', header=False, index=False)
