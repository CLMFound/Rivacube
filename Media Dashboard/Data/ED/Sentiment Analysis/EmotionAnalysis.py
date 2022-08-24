import pandas as pd
from nrclex import NRCLex
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Read csv
file = pd.read_csv('filename')

#Convert from float to string if needed
file['Text'] = file['Text'].astype(str)
file['emotions'] = file['Text'].apply(lambda x: NRCLex(x).affect_frequencies)
file.head()


# Give each emotion a column
file = pd.concat([file.drop(['emotions'], axis = 1), file['emotions'].apply(pd.Series)], axis = 1)
file.head()
file.to_csv("Emotion.csv")

#Create column month
file['Date Created'] = pd.to_datetime(file['Date Created'])
file['Month'] = file['Date Created'].apply(lambda x: x.replace(day=1)+ relativedelta(months=1)
                                 if x.day > 15
                                 else x.replace(day=1))
file['Month'] = file['Month'].dt.date
file.to_csv("Emotion.csv")

#Mean sentiment per month of fear
months = file.groupby('Month')['fear'].sum()
monthly = pd.DataFrame(months)
monthly.to_csv("MonthlyEmotion.csv")

#Plot monthly mean sentiment of fear
def plot_df(x, y, title="", xlabel='Date', ylabel='Fear Sentiment', dpi=100):
    plt.figure(figsize=(16, 8), dpi=dpi)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
    plt.plot(x, y, color='tab:red',marker='o')
    plt.xticks(rotation=45)
    plt.gca().set(title=title, xlabel=xlabel, ylabel=ylabel)
    plt.savefig('DroughtNRCFear.png')
    plt.show()

plot_df(x=monthly.index, y=monthly.fear, title='Sentiment of Fear Per Month')



