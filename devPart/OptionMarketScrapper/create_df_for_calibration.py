import pandas as pd
import numpy as np
from tqdm import tqdm
import glob

global EXPIRATION_DATE
global UNDERLYING

def preprocess_dataframe(path: str, _UNDR) -> pd.DataFrame:
    # Only CALL Options
    strike = int(path.split('_')[1].split('.')[0])
    dataframe = pd.read_csv(path)
    dataframe.index = pd.to_datetime(dataframe["minutesTime"])
    dataframe = dataframe.loc[dataframe.index > UNDERLYING.index[0]]
    dataframe['tau'] = EXPIRATION_DATE - pd.to_datetime(dataframe["minutesTime"])
    dataframe['tau'] = dataframe['tau'].apply(lambda x: x.total_seconds() / (365.25*24*60*60))
    dataframe['days'] = (dataframe['tau'] * 365.25).astype(int)

    _underl = list()
    for index in tqdm(range(dataframe.shape[0])):
        try:
            _underl.append(_UNDR[_UNDR["minutesTime"] == dataframe.iloc[index]["minutesTime"]].iloc[0]["Open"])
        except IndexError:
            pass

    dataframe["underlying_price"] = _underl
    dataframe["strike_price"] = strike
    dataframe['payoff'] = np.maximum(np.array(_underl) - strike, 0.0)
    dataframe["last_price"] = dataframe["Open"]
    dataframe["mid_price"] = dataframe["last_price"]

    return dataframe


if __name__ == '__main__':

    UNDERLYING = pd.read_csv('saveStorage/16DEC22/underlyingBars.csv').copy()
    UNDERLYING['minutesTime'] = pd.to_datetime(UNDERLYING['minutesTime'])
    UNDERLYING.index = UNDERLYING["minutesTime"]
    UNDERLYING = UNDERLYING.resample('1T').first().fillna(method='ffill')
    UNDERLYING["minutesTime"] = UNDERLYING.index

    EXPIRATION_DATE = pd.Timestamp(year=2022, month=12, day=17)

    # '2022-12-02 08:04:00'
    print(UNDERLYING)
    list_of_dataframes = []
    for path in glob.glob("saveStorage/16DEC22/*"):
        if "underlyingBars.csv" not in path:
            list_of_dataframes.append(preprocess_dataframe(path, UNDERLYING))

    dataframe = pd.concat(list_of_dataframes)
    dataframe.to_csv("result.csv")
    
    # preprocess_dataframe('saveStorage/16DEC22/strikeBars_15000.csv', UNDERLYING)
