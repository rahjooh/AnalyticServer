from plotly.graph_objs import *
from plotly.offline import plot as offpy
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
import plotly.graph_objs as go
import os
import numpy as np
from plotly.plotly import plotly


def visualize(G, node_size, weights, filename="netwrokx", title=""):
    keys = G.nodes()
    values = range(len(G.nodes()))
    dictionary = dict(zip(keys, values))
    inv_map = {v: k for k, v in dictionary.items()}
    G = nx.relabel_nodes(G, dictionary)
    try:
        pos = graphviz_layout(G)
    except:
        pos = graphviz_layout(G)

    edge_trace = Scatter(
        x=[ ],
        y=[ ],
        line=Line(width=[ ], color='rgba(136, 136, 136, .8)'),
        hoverinfo='none',
        mode='lines')

    for edge in G.edges():
        x0, y0 = pos[ edge[ 0 ] ]
        x1, y1 = pos[ edge[ 1 ] ]
        edge_trace[ 'x' ] += [ x0, x1, None ]
        edge_trace[ 'y' ] += [ y0, y1, None ]

    for weight in weights:
        edge_trace[ 'line' ][ 'width' ].append(weight)

    node_trace = Scatter(
        x=[ ],
        y=[ ],
        text=[ ],
        mode='markers+text',
        textfont=dict(family='Calibri (Body)', size=25, color='black'),
        opacity=100,
        # hoverinfo='text',
        marker=Marker(
            showscale=True,
            # colorscale options
            # 'Greys' | 'Greens' | 'Bluered' | 'Hot' | 'Picnic' | 'Portland' |
            # Jet' | 'RdBu' | 'Blackbody' | 'Earth' | 'Electric' | 'YIOrRd' | 'YIGnBu'
            colorscale='Hot',
            reversescale=True,
            color=[ ],
            size=[ ],
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line=dict(width=2)))

    for node in G.nodes():
        x, y = pos[ node ]
        node_trace[ 'x' ].append(x)
        node_trace[ 'y' ].append(y)

    for adjacencies in G.adjacency_list():
        node_trace[ 'marker' ][ 'color' ].append(len(adjacencies))

    for node in G.nodes():
        node_trace[ 'text' ].append(inv_map[ node ])

    for size in node_size:
        node_trace[ 'marker' ][ 'size' ].append(size)

    fig = Figure(data=Data([ edge_trace, node_trace ]),
                 layout=Layout(
                     title='<br>' + title,
                     titlefont=dict(size=16),
                     showlegend=False,
                     width=1500,
                     height=800,
                     hovermode='closest',
                     margin=dict(b=20, l=350, r=5, t=200),
                     # family='Courier New, monospace', size=18, color='#7f7f7f',
                     annotations=[ dict(
                         text="",
                         showarrow=False,
                         xref="paper", yref="paper",
                         x=0.005, y=-0.002) ],
                     xaxis=XAxis(showgrid=False, zeroline=False, showticklabels=False),
                     yaxis=YAxis(showgrid=False, zeroline=False, showticklabels=False)))

    offpy(fig, filename=filename, auto_open=True, show_link=False)


def scale_color_graph_visualize(Graph, color_list,
                                filename="plotly",
                                colorscale='Hot',
                                title="",
                                reverse_scale=True,
                                node_size=20):
    keys = Graph.nodes()
    values = range(len(Graph.nodes()))
    dictionary = dict(zip(keys, values))
    inv_map = {v: k for k, v in dictionary.items()}
    G = nx.relabel_nodes(Graph, dictionary)
    pos = graphviz_layout(G)

    edge_trace = Scatter(
        x=[ ],
        y=[ ],
        line=Line(width=3, color='rgba(136, 136, 136, .8)'),
        # hoverinfo=[],
        text=[ ],
        mode='lines+markers+text')

    for edge in G.edges():
        x0, y0 = pos[ edge[ 0 ] ]
        x1, y1 = pos[ edge[ 1 ] ]
        edge_trace[ 'x' ] += [ x0, x1, None ]
        edge_trace[ 'y' ] += [ y0, y1, None ]
        # edge_trace['hoverinfo'].append('ddd')
        # edge_trace['text'].append('888888888')

    # def make_text(X):
    #    return ''

    node_trace = Scatter(
        x=[ ],
        y=[ ],
        text=[ ],
        # text=X.apply(make_text, axis=1).tolist(),
        mode='markers+text',
        # mode='markers',
        textfont=dict(family='Calibri (Body)', size=15, color='black'),
        opacity=100,
        hoverinfo='none',
        marker=Marker(
            showscale=True,
            # colorscale options
            # 'Greys' | 'Greens' | 'Bluered' | 'Hot' | 'Picnic' | 'Portland' |
            # Jet' | 'RdBu' | 'Blackbody' | 'Earth' | 'Electric' | 'YIOrRd' | 'YIGnBu'
            colorscale=colorscale,
            reversescale=reverse_scale,
            color=[ ],
            size=node_size,
            colorbar=dict(
                thickness=15,
                title='',
                xanchor='left',
                titleside='right'
            ),
            line=dict(width=2)))

    for node in G.nodes():
        x, y = pos[ node ]
        node_trace[ 'x' ].append(x)
        node_trace[ 'y' ].append(y)
    # for node in G.nodes():
    # node_trace['hoverinfo'][0]='1212121212121'#.append('888888880')

    for adjacencies, color in zip(G.adjacency_list(), color_list):
        node_trace[ 'marker' ][ 'color' ].append(color)

    for node in G.nodes():
        node_trace[ 'text' ].append(inv_map[ node ])

    fig = Figure(data=Data(
        [ edge_trace, node_trace ]),
        layout=Layout(
            title='<br>' + title,
            titlefont=dict(size=16),
            showlegend=False,
            width=1500,
            height=800,
            # hovermode='closest',
            hovermode='closest',
            margin=dict(b=20, l=400, r=5, t=200),
            # family='Courier New, monospace', size=18, color='#7f7f7f',
            annotations=[ dict(
                text="",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002) ],
            xaxis=XAxis(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=YAxis(showgrid=False, zeroline=False, showticklabels=False)))

    # j=0
    # for x in fig.data:
    #    print(x)
    # for node in G.nodes():
    #    fig.data[0]['text'].append('asasa'+str(j))
    #    j+=1

    offpy(fig, filename=filename, auto_open=True, show_link=False)


def color_graph_visualize(Graph, color_list, filename="plotly", title=""):
    keys = Graph.nodes()
    values = range(len(Graph.nodes()))
    dictionary = dict(zip(keys, values))
    inv_map = {v: k for k, v in dictionary.items()}
    G = nx.relabel_nodes(Graph, dictionary)
    pos = graphviz_layout(G)

    edge_trace = Scatter(
        x=[ ],
        y=[ ],
        line=Line(width=3, color='rgba(136, 136, 136, .8)'),
        hoverinfo='none',
        mode='lines',
        text=[ ])

    for edge in G.edges():
        x0, y0 = pos[ edge[ 0 ] ]
        x1, y1 = pos[ edge[ 1 ] ]
        edge_trace[ 'x' ] += [ x0, x1, None ]
        edge_trace[ 'y' ] += [ y0, y1, None ]
        edge_trace[ 'text' ].append('label')

    node_trace = Scatter(
        x=[ ],
        y=[ ],
        text=[ ],
        mode='markers+text',
        textfont=dict(family='Calibri (Body)', size=15, color='black'),
        opacity=100,
        hoverinfo='text',
        marker=Marker(
            color=[ ],
            size=30,
            line=dict(width=2)))

    for node in G.nodes():
        x, y = pos[ node ]
        node_trace[ 'x' ].append(x)
        node_trace[ 'y' ].append(y)

    for adjacencies, color in zip(G.adjacency_list(), color_list):
        node_trace[ 'marker' ][ 'color' ].append(color)

    for node in G.nodes():
        node_trace[ 'text' ].append(inv_map[ node ])

    fig = Figure(data=Data([ edge_trace, node_trace ]),
                 layout=Layout(
                     title='<br>' + title,
                     titlefont=dict(size=16),
                     showlegend=False,
                     width=1500,
                     height=800,
                     hovermode='closest',
                     margin=dict(b=20, l=350, r=5, t=200),
                     # family='Courier New, monospace', size=18, color='#7f7f7f',
                     # paper_bgcolor='#7f7f7f',
                     # plot_bgcolor='#c7c7c7',
                     annotations=[ dict(
                         text="",
                         showarrow=False,
                         xref="paper", yref="paper",
                         x=0.005, y=-0.002) ],
                     xaxis=XAxis(showgrid=False, zeroline=False, showticklabels=False),
                     yaxis=YAxis(showgrid=False, zeroline=False, showticklabels=False)))

    offpy(fig, filename=filename, auto_open=True, show_link=False)

def three_d_nework_graph(x_positions, y_positions, z_positions, colors, labels="unknown"):
    trace2 = Scatter3d(x=x_positions,
                       y=y_positions,
                       z=z_positions,
                       mode='markers',
                       name='actors',
                       marker=Marker(symbol='dot',
                                     size=6,
                                     color=colors,
                                     colorscale='Viridis',
                                     line=Line(color='rgb(50,50,50)', width=0.5)
                                     ),
                       text=labels,
                       hoverinfo='text'
                       )
    axis = dict(showbackground=True,
                showline=True,
                zeroline=True,
                showgrid=True,
                showticklabels=True,
                title=''
                )
    layout = Layout(
        title="Network of coappearances of documents in the whole repository(3D visualization)",
        width=1000,
        height=1000,
        showlegend=False,
        scene=Scene(
            xaxis=XAxis(axis),
            yaxis=YAxis(axis),
            zaxis=ZAxis(axis),
        ),
        margin=Margin(
            t=100
        ),
        hovermode='closest',
        annotations=Annotations([
            Annotation(
                showarrow=False,
                text="",
                xref='paper',
                yref='paper',
                x=0,
                y=0.1,
                xanchor='left',
                yanchor='bottom',
                font=Font(
                    size=14
                )
            )
        ]), )
    data = Data([ trace2 ])
    fig = Figure(data=data, layout=layout)

    offpy(fig, filename="dd", auto_open=True, show_link=False)


def time_series_plot(x, y):
    data = [ go.Scatter(
        x=x,
        y=y) ]

    '''
    layout = go.Layout(xaxis=dict(
        range=[to_unix_time(datetime.datetime(2013, 10, 17)),
               to_unix_time(datetime.datetime(2013, 11, 20))]
    ))
    '''
    fig = go.Figure(data=data)

    offpy(fig, filename="dd", auto_open=True, show_link=False)

def bar_chart_plot(x, y, title, out_path):
    data = [ go.Bar(
        x=x,
        y=y
    ) ]

    layout = go.Layout(title=title)
    fig = go.Figure(data=data, layout=layout)

    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)


def histogram(x):
    data = [go.Histogram(x=x, histnorm='probability', nbinsx=100) ]
    fig = go.Figure(data=data)
    offpy(fig, filename="histogram.html", auto_open=True, show_link=False)


def scatter(x, y, title, axis_labels, out_path):
    trace = go.Scatter(x=x,
                       y=y,
                       mode='markers')
    layout = go.Layout(
        title=title,
        hovermode='closest',
        xaxis=dict(
            title=axis_labels[ 0 ],
            ticklen=5,
            zeroline=False,
            gridwidth=2,
        ),
        yaxis=dict(
            title=axis_labels[ 1 ],
            ticklen=5,
            gridwidth=2,
        ),
        showlegend=False
    )

    data = [ trace ]
    fig = go.Figure(data=data, layout=layout)
    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)


def scatter3d(traces, columns, title, out_path):
    data = []
    for trace in traces:
        data.append(go.Scatter3d(
                        x=trace[columns[0]].values,
                        y=trace[columns[1]].values,
                        z=trace[columns[2]].values,
                        mode='markers',

                        marker=dict(
                            size=6,
                            #line=dict(
                                #color='rgba(217, 217, 217, 0.14)',
                                #width=5
                            #),
                            opacity=0.8
                        )
                    ))
    layout = go.Layout(
        title=title,
        #hovermode='closest',
        scene = Scene(
            xaxis = XAxis(title=columns[0]),
            yaxis = YAxis(title=columns[1]),
            zaxis = ZAxis(title=columns[2])
        ),
        showlegend=True
    )
    fig = go.Figure(data=data, layout=layout)

    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)
    #with open("scatter3.txt", 'w') as fw:
    #    fw.write(offpy(fig, show_link=False, include_plotlyjs=False, output_type='div'))
    return offpy(fig, show_link=False, include_plotlyjs=False, output_type='div')

def scatter_by_guild(traces, columns, title, labels, out_path):
    data = [ ]
    for trace in traces:
        #print(trace["df"]["city_name"].values)
        data.append(go.Scatter(
            x=trace["df"][ columns[ 0 ] ].values,
            y=trace["df"][ columns[ 1 ] ].values,
            name=trace["senf_name"],
            text=trace["df"]["city_name"].values,
            mode="markers",
            marker=dict(
                size=8,
                # color='rgba(255, 182, 193, .9)',

            )

        ))

    layout = go.Layout(
        title=title,
        hovermode='closest',
        xaxis=dict(
            title=labels[ 0 ],
            ticklen=5,
            zeroline=False,
            gridwidth=2,
        ),
        yaxis=dict(
            title=labels[ 1 ],
            ticklen=5,
            gridwidth=2,
        ),
        showlegend=True
    )

    fig = go.Figure(data=data, layout=layout)
    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)
    return offpy(fig, show_link=False, include_plotlyjs=False, output_type='div')

def scatter_by_cluster(traces, columns, title, axis_labels, out_path):
    data = []
    for trace in traces:
        data.append(go.Scatter(
            x=trace[columns[0]].values,
            y=trace[columns[1]].values,
            name=trace[columns[2]].values[0],
            mode="markers",
            marker=dict(
                size=8,
                # color='rgba(255, 182, 193, .9)',

            )

        ))

    layout = go.Layout(
        title=title,
        hovermode='closest',
        xaxis=dict(
            title=axis_labels[ 0 ],
            ticklen=5,
            zeroline=False,
            gridwidth=2,
        ),
        yaxis=dict(
            title=axis_labels[ 1 ],
            ticklen=5,
            gridwidth=2,
        ),
        showlegend=True
    )

    fig = go.Figure(data=data, layout=layout)
    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)
    return offpy(fig, show_link=False, include_plotlyjs=False, output_type='div')


def segments_plot(y, vertical_lines, title, out_path):
    trace = go.Scatter(
        y=y,
    )

    lines = [ ]
    for vertical_line in vertical_lines:
        lines.append({
            'type': 'line',
            'x0': 0,
            'y0': vertical_line,
            'x1': len(y),
            'y1': vertical_line,
            'opacity': 0.7,
            'line': {
                'color': 'red',
                'width': 2.5,
            },
        })
    layout = {
        'title': title,
        'shapes': lines,
        'yaxis': {'title': "Value"},
        'xaxis': {'title': "Sorted Index"}
    }

    data = [ trace ]
    fig = {
        'data': data,
        'layout': layout,
    }
    # fig = go.Figure(data=data, layout=layout)
    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)


def stacked_bar_char(feature_counts_df, title, out_path):
    data = []

    feature_names = list(feature_counts_df.columns.values)
    for index, row in feature_counts_df.iterrows():
        counts = [ ]
        for feature_name in feature_names:
            counts.append(row[ feature_name ])
        trace = go.Bar(
            x=feature_names,
            y=counts,
            name=str(index)
        )
        data.append(trace)

    layout = go.Layout(
        barmode='stack'
    )
    fig = go.Figure(data=data, layout=layout)
    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)


def boxplot(x_data, y_data, title, out_path="."):
    traces = []

    for xd, yd in zip(x_data, y_data):
        traces.append(go.Box(
            y=yd,
            name="_"+str(xd)+"_",
            boxpoints='all',
            jitter=0.5,
            whiskerwidth=0.2,
            #fillcolor=cls,
            marker=dict(
                size=2,
            ),
            line=dict(width=1),
        ))


    layout = go.Layout(
        title=title,
        yaxis=dict(
            autorange=True,
            showgrid=True,
            zeroline=True,
            #dtick=5,
            gridcolor='rgb(255, 255, 255)',
            gridwidth=1,
            zerolinecolor='rgb(255, 255, 255)',
            zerolinewidth=2,
        ),
        margin=dict(
            l=40,
            r=30,
            b=80,
            t=100,
        ),
        #paper_bgcolor='rgb(243, 243, 243)',
        #plot_bgcolor='rgb(243, 243, 243)',
        showlegend=True,
    )

    fig = go.Figure(data=traces, layout=layout)
    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)
    return offpy(fig, show_link=False, include_plotlyjs=False, output_type='div')


def heamap(x, y, z, title, out_path="."):
    trace = go.Heatmap(z=z,
                       x=x,
                       y=y,
                       colorscale='Jet')
    data = [trace]

    layout = dict(
                  yaxis=dict(tickmode="array"),

                  )

    fig = go.Figure(data=data, layout=layout)

    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)
    return offpy(fig, show_link=False, include_plotlyjs=False, output_type='div')


if __name__ == "__main__":
    pass