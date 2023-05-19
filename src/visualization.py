import pandas as pd
import numpy as np

from sklearn.metrics import accuracy_score, fbeta_score, recall_score, precision_score

def nice_summary(df):

    return pd.concat([    
                pd.DataFrame({
                'Dtype': df.dtypes,
                'nunique': df.nunique(),
                'Non-Null Count': df.count(),
                'Missing': df.isnull().sum(),        
                'Missing %': round((df.isnull().sum()/df.shape[0])*100, 2),
                'Zero Count': (df == 0).sum(),
                })
                ,df.describe().round(2).T.iloc[:,1:]
            ], axis=1) \
            .fillna('-') \
            .reset_index() \
            .rename(columns={'index': 'Columns'}) \
            .replace({'Missing': 0, 'Missing %': 0}, '-')