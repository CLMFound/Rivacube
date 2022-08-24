import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.interpolate as interp


#Read File
file = pd.read_csv('file with open prices')
file2 = pd.read_csv('file with emotion analysis')

#Plot Open Prices Per Month
def plot_df(x, y, title="", xlabel='Date', ylabel='Price', dpi=100):
    plt.figure(figsize=(16, 8), dpi=dpi)
    plt.plot(x, y, color='tab:red')
    plt.xticks(np.arange(0, len(file.Date), 12))
    plt.xticks(rotation=45)
    plt.gca().set(title=title, xlabel=xlabel, ylabel=ylabel)
    plt.savefig('DanonePerMonth.png')
    plt.show()

plot_df(x=file.Date, y=file.Open, title='Danone Open Prices Per Month')

#######################
#Scatter Plot
######################


#Plot Scatter with correation to drought
x = file['Open']
y = file2['fear']

# Make size of y equal to the size of x
y_inter = interp.interp1d(np.arange(y.size), y)
y_ = y_inter(np.linspace(0,y.size-1,x.size))

#Plot scatter
plt.figure(figsize=(16, 8))
plt.scatter(x, y_, color='tab:red')
#Trend Line
z = np.polyfit(x, y_, 1)
p = np.poly1d(z)
plt.plot(x, p(x), "r--")
#Labels
plt.title("Danone Open Prices Correlation with Drought Emotion Sentiment")
plt.xlabel("Open Prices")
plt.ylabel("Fear Sentiment")
plt.xticks(rotation=45)
plt.savefig('Danone Correlation.png')
plt.show()

