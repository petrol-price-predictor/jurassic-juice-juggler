import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

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

def set_rc_params(front_color='white', bg_color='#232742'):
    # Set text color and axes color
    plt.rcParams['text.color'] = front_color
    plt.rcParams['axes.labelcolor'] = front_color
    plt.rcParams['xtick.color'] = front_color
    plt.rcParams['ytick.color'] = front_color
    plt.rcParams['axes.edgecolor'] = front_color
    plt.rcParams['grid.color'] = front_color

    # Set the background color
    plt.rcParams['axes.facecolor'] = bg_color

    # Set the font
    plt.rcParams['font.sans-serif'] = 'Arial'

    # Set the grid alpha of the charts grid
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.alpha'] = 0.1
    

def set_plot_options(figure, axis):

    axis.margins(x=0)
    plt.tight_layout()

    #set legend text color
    legend = axis.get_legend()
    for text in legend.get_texts():
        text.set_color('white')
    figure.patch.set_alpha(0.0)

