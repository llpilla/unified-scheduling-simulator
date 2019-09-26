"""Plotter module. Provides histogram and bar methods for plotting."""
import os                       # for creating directories
import matplotlib.pyplot as plt # for plots

# Class to plot load information
class Plotter:
    """Class to plot load information"""
    def __init__(self, path, data):
        """Defines the parameters for plotting figures"""
        self.path = path
        try:
            self.data = data
            loads = data.loads
            mean_load = data.mean_load
        except AttributeError:
            print("Error: the object is not yet plottable")
        try:    # Creates a directory for storing images, if one does not exist yet
            os.mkdir(self.path)
        except OSError:
            pass

    def histogram(self):
        """Plots a load histogram"""
        data = self.data
        plt.hist(data.loads, bins='auto', color=data.color, alpha=0.75, rwidth=1)
        plt.grid(axis='y', alpha=0.85)
        plt.xlabel(data.name+' load')
        plt.ylabel('Frequency (#'+data.name+'s)')
        plt.title(data.name + ' load histogram')
        plt.savefig(self.path + 'fig-histogram-' + data.name.lower() + 's.pdf', bbox_inches='tight')
        plt.clf()

    def bars(self, ordered=False, zoom=False):
        """Plots a load bar graph"""
        data = self.data
        if ordered: #if ordered, gets a copy of the data and sorts it
            loads = list(data.loads)
            loads.sort()
        else:
            loads = data.loads
        # x label: informs if data is sorted or not
        if ordered:
            plt.xlabel(data.name+' ordered by load')
        else:
            plt.xlabel(data.name+' id')
        plt.bar(range(0, data.num), loads, color=data.color)
        # y label: can inform if the data is zoomed or not
        plt.ylabel(data.name+' load'+(' (zoomed)' if zoom else ''))
        # title: can inform if the data is zoomed and sorted
        sor = 'Sorted ' if ordered else ''
        plt.title(sor+data.name+' loads'+(' (zoomed)' if zoom else ''))
        if zoom: # applies zoom if necessary
            plt.ylim(data.min_load*0.99, data.max_load*1.01)
        if data.num < 25: # small check to make sure the ticks are not one over the other
            plt.xticks(range(0, data.num))
        plt.axhline(y=data.mean_load, xmin=-0.5, xmax=data.num+0.5, linewidth=1, color='r')
        plt.text(-1, data.mean_load, r'$\bar L$', color='r')
        plt.axhline(y=data.median_load, xmin=-0.5, xmax=data.num+0.5, linewidth=1, color='k')
        plt.text(data.num + 1, data.median_load, 'Med', color='k')
        fig_name = self.path + 'fig-bars-' + data.name.lower() + 's'
        fig_name = fig_name + ('-sorted' if ordered else '')
        fig_name = fig_name + ('-zoomed' if zoom else '')
        fig_name = fig_name + '.pdf'
        plt.savefig(fig_name, bbox_inches='tight')
        plt.clf()
