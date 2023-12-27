import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

filepath = '/home/nml/nml_nhp/nml_plexon-ros_workspace/data/231117_163448/Max_2023_11_17_4.h5'
#filepath = '/home/nml/nml_nhp/nml_plexon-ros_workspace/data/231117_164353/Max_2023_11_17_5.h5'
df = pd.read_hdf(filepath,'/data')

# Get events where only force data and pico events occured. Reset indexing
#force_pico_df = df.loc[df.index[df['topic'].isin(['/robot/command/force','/pico/pulse_bit'])]]
#force_pic_df = force_pico_df.reset_index()

# Get events where only force data and pico events occured. Reset indexing
pos_df = df.loc[df.index[df['topic'].isin(['/robot/feedback/position','/pico/pulse_bit'])]]
pos_df = pos_df.reset_index()

# Get indices of just pico pulse events
#pulse_events = force_pico_df.loc[force_pico_df["topic"] == '/pico/pulse_bit']
pulse_events = pos_df.loc[pos_df["topic"] == '/pico/pulse_bit']
p = pulse_events['time_ns'].to_list()

t = (pos_df['time_ns'] - pos_df['time_ns'][0])/1e9

# Plot position data as a sanity check
plt.plot(t, pos_df['z'])
for q in p:
    plt.axvline(x=(q - pos_df['time_ns'][0])/1e9, color='r')
    
plt.title('Robot Position [z]')
plt.xlabel('Time (s)')
plt.ylabel('m')
plt.show()


# Grab the next N events published to the ROS2 network starting from each pulse event
i = 1
N = 1000
if False:
    x = force_pico_df.iloc[p[0]-N:p[0]+N]['x'].to_list()
    y = force_pico_df.iloc[p[0]-N:p[0]+N]['y'].to_list()
    z = force_pico_df.iloc[p[0]-N:p[0]+N]['z'].to_list()
    t = force_pico_df.iloc[p[0]-N:p[0]+N]['time_ns']/1e9
else:
    x = pos_df.iloc[p[i]-N:p[i]+N]['x'].to_list()
    y = pos_df.iloc[p[i]-N:p[i]+N]['y'].to_list()
    z = pos_df.iloc[p[i]-N:p[i]+N]['z'].to_list()
    t = pos_df.iloc[p[i]-N:p[i]+N]['time_ns']/1e9
    
t = t.to_list()

# np.diff(t).mean()
# >> 0.0009513001492048515 seconds == 950us, fast update rate

magnitude = np.sqrt(np.sum(np.square([x, y, z]),axis=0))
magnitude = y
plt.plot(t, magnitude)
plt.axvline(x=t[N], color='r')
plt.show()




