import dash
from dash import dcc, html, Input, Output, State
from dash.exceptions import PreventUpdate
import pandas as pd
import plotly.express as px
import json
import threading
import requests
from urllib.parse import urlparse

import data_retriever
import loader
from assessment import evaluator
from console_print import c_print, MyColors as Color
import event_bus
from from_database import DatabaseManager

app = dash.Dash(__name__, assets_folder='assets')

data = pd.read_csv('assets/prova.csv', delimiter=',')

data2 = pd.read_csv('assets/prova3.csv', delimiter=',')

know_urls = {'Comune di Bitonto': 'https://www.opendata.maggioli.cloud/<c_a893>',
             'Comune di Massafra': 'https://dati.puglia.it/ckan<c_f027>',
             'Comune di Crispiano': 'http://dati.comune.crispiano.ta.it'}

deepness_value = ['Valuta tutti', 'Valuta scaduti', 'Valuta nuovi']

text_values = {"text4": ".", "text": "LOADING..", "text1": "loading..", "text2": "loading..", "text3": "loading.."}

expanded = False

use_cloud = False

use_geo = False

in_assessment = False
store = dcc.Store(id='state-store', data={'in_assessment': in_assessment})


def find_portal(url):
    if not urlparse(url).scheme:
        url_ = "https://" + url
        c_print.myprint(f"URL INPUT '{url}' MISS SCHEMA. Trying with {url_}", Color.RED, 0)
    else:
        url_ = url

    try:
        data_retriever.DataRetriever(url_).try_ckan()
        return url_

    except data_retriever.DataRetrieverError:
        url__ = url_ + ('/' if not url_.endswith('/') else '') + 'ckan'
        c_print.myprint(f"Ckan not Compatible with Input URL '{url}'. Trying with {url__}", Color.RED, 0)
        try:
            data_retriever.DataRetriever(url__).try_ckan()
            return url__
        except data_retriever.DataRetrieverError:
            c_print.myprint("Ckan not Compatible with Portal", Color.RED, 0)
            raise data_retriever.DataRetrieverError


def do_work(sel_port_name, sel_port, sel_deep):
    loader_ = loader.Loader(use_cloud, use_geo)
    loader_.load_system_for_portal()
    database = loader_.get_database()
    known_portals = database.get_portal_list()

    url = sel_port
    deepness = sel_deep

    dataset_list = []
    if url.find('<') != -1:

        holder = url[url.find('<') + 1:-1]
        portal_id = find_portal(url[:url.find('<')])
        if portal_id not in known_portals:
            database.save_portal(portal_id, '')

        database.set_portal_id(portal_id)
        event = {'type': 'portal', 'portal': portal_id}
        event_bus.event_bus.publish(event)

        api_url = f"{portal_id}/api/3/action/package_search"
        search_params = {
            "q": f"holder_identifier:{holder}",
            "fl": "id",
            "rows": 1000
        }

        response = requests.get(api_url, params=search_params)
        if response.status_code == 200:
            payload = response.json()
            for dataset in payload['result']['results']:
                dataset_list.append(dataset['id'])

        portal_instance = data_retriever.DataRetriever(portal_id)

    else:
        portal_id = find_portal(url)
        if portal_id not in known_portals:
            database.save_portal(portal_id, '')

        database.set_portal_id(portal_id)
        event = {'type': 'portal', 'portal': portal_id}
        event_bus.event_bus.publish(event)

        portal_instance = data_retriever.DataRetriever(portal_id)
        dataset_list = portal_instance.get_package_list()

    event = {'type': 'update_text_toplevel', 'text4': 'Working on ' + str(sel_port_name)}
    event_bus.event_bus.publish(event)

    known_dataset = database.get_dataset_list()
    expired_dataset = database.get_expired_datasets_list()
    portal_dataset = dataset_list

    unknown_dataset = list(set(portal_dataset).difference(known_dataset))
    evaluator_ = evaluator.Evaluator()

    if deepness == 1:
        evaluator_.start_eval(loader_.get_cloud(), database, portal_dataset, portal_instance, loader_.get_geo())
    elif deepness == 2:
        evaluator_.start_eval(loader_.get_cloud(), database, expired_dataset, portal_instance,
                              loader_.get_geo())
    elif deepness == 3:
        evaluator_.start_eval(loader_.get_cloud(), database, unknown_dataset, portal_instance,
                              loader_.get_geo())

    DatabaseManager()

    event_bus.event_bus.publish(
        {'type': 'update_text_toplevel', 'text': f'Assessment Done', 'text1': '.', 'text2': '.',
         'text3': '.'})

    store.data['in_assessment'] = False


@app.callback(
    Output('dataset_id-dropdown', 'options'),
    Input('holder-dropdown', 'value')
)
def update_dataset_id_options(selected_holder):
    # Filtra il DataFrame in base agli holder selezionati
    filtered_df = data[data['holder'].isin(selected_holder)]

    # Ottieni le opzioni distinte per il dropdown dataset_id
    dataset_id_options = [{'label': id_, 'value': id_} for id_ in filtered_df['dataset_id'].unique()]

    return dataset_id_options


# AGGIUNGERE I PULSANTI NEL DIV PER SELEZIONARE AUTOMATICAMENTE TUTTI GLI HOLDER E TUTTI GLI ID
@app.callback(
    Output('dataset_id-dropdown', 'value'),
    Input('select-all-id-button', 'n_clicks'),
    State('dataset_id-dropdown', 'options')
)
def select_all_filters(select_all_id_clicks, id_options):
    if select_all_id_clicks % 2 == 1:
        selected_id = [option['value'] for option in id_options]
    else:
        selected_id = []

    return selected_id


# AGGIUNGERE I PULSANTI NEL DIV PER SELEZIONARE AUTOMATICAMENTE TUTTI GLI HOLDER E TUTTI GLI ID
@app.callback(
    Output('holder-dropdown', 'value'),
    Input('select-all-holder-button', 'n_clicks'),
    State('holder-dropdown', 'options'),
)
def select_all_filters(select_all_holder_clicks, holder_options):
    # Gestisci il pulsante "Seleziona Tutti gli holder"
    if select_all_holder_clicks % 2 == 1:
        selected_holder = [option['value'] for option in holder_options]
    else:
        selected_holder = []

    return selected_holder


@app.callback(
    Output('histogram', 'figure'),
    [Input('holder-dropdown', 'value'),
     Input('dataset_id-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_histogram(selected_holder, selected_id, n_intervals):
    data = pd.read_csv('assets/prova.csv', delimiter=',')

    # Filtra il DataFrame in base agli holder e agli ID dataset selezionati
    filtered_df = data[(data['holder'].isin(selected_holder)) & (data['dataset_id'].isin(selected_id))]

    if filtered_df.empty:
        return {
            'data': [],
            'layout': {
                'title': 'No Data',
                'xaxis': {'title': 'Metrica'},
                'yaxis': {'title': 'Punteggio medio'}
            }
        }

    fig = px.histogram(
        filtered_df, x='METRICA', y='punteggio', color='PRINCIPIO',
        histfunc='avg', barmode='group'
    )

    fig.update_layout(
        xaxis=dict(categoryorder='array', categoryarray=data['METRICA'].unique())
    )

    return fig


# callback pulsante avvio valutazione
@app.callback(
    [Output('avvia-valutazione-button', 'children'),
     Output('state-store', 'data')],
    [Input('avvia-valutazione-button', 'n_clicks'),
     Input('valuta-tutti-button', 'n_clicks'),
     Input('state-store', 'data')],
    State('portale-dropdown', 'value'),
    State('valutazione-dropdown', 'value'),
    prevent_initial_call=True
)
def start_valuation(n_clicks_avvia, n_clicks_valuta_tutti, store_data, selected_portal, selected_valuation_type):
    if not store_data['in_assessment']:
        if n_clicks_avvia is None and n_clicks_valuta_tutti is None:
            raise PreventUpdate

        if n_clicks_valuta_tutti and n_clicks_valuta_tutti > 0:
            # Esegui la valutazione per tutti i portali
            for name in know_urls.keys():
                my_thread = threading.Thread(target=do_work, args=(name, know_urls[name], 1))
                my_thread.start()
                my_thread.join()

            # Mostra un messaggio appropriato
            return (f"Avviata valutazione per tutti i portali",
                    {'in_assessment': True})
        elif n_clicks_avvia and n_clicks_avvia > 0:
            name_ = ''
            for name in know_urls.keys():
                if selected_portal == know_urls[name]:
                    name_ = name
            # Esegui la valutazione per il portale specifico
            my_thread = threading.Thread(target=do_work, args=(name_, selected_portal, selected_valuation_type))
            my_thread.start()

            selected_portal_key = next((key for key, value in know_urls.items() if value == selected_portal), None)

            # Usa l'indice direttamente per ottenere la stringa desiderata
            if selected_valuation_type == 1:
                valutazione_type_string = 'Valuta Tutti'
            elif selected_valuation_type == 2:
                valutazione_type_string = 'Valuta Scaduti'
            elif selected_valuation_type == 3:
                valutazione_type_string = 'Valuta Nuovi'
            else:
                valutazione_type_string = 'Valuta Sconosciuta'

            return (f"Avviata valutazione per il portale {selected_portal_key} con opzione {valutazione_type_string}",
                    {'in_assessment': True})
        else:
            raise PreventUpdate
    else:
        return f"C'è gia una valutazione in corso attendere", {'in_assessment': True}


@app.callback(
    [Output('text4', 'children'),
     Output('text', 'children'),
     Output('text1', 'children'),
     Output('text2', 'children'),
     Output('text3', 'children'),
     Output('text4', 'style'),
     Output('text', 'style'),
     Output('text1', 'style'),
     Output('text2', 'style'),
     Output('text3', 'style')],
    Input('interval-component2', 'n_intervals')
)
def update_h2(n_intervals):
    if text_values != {"text4": ".", "text": "LOADING..", "text1": "loading..", "text2": "loading..", "text3": "loading.."}:
        return (
        text_values['text4'], text_values['text'], text_values['text1'], text_values['text2'], text_values['text3'],
        {'color': 'black'}, {'color': 'black'}, {'color': 'black'}, {'color': 'black'}, {'color': 'black'})
    else:
        return (
        text_values['text4'], text_values['text'], text_values['text1'], text_values['text2'], text_values['text3'],
        {'color': 'white'}, {'color': 'white'}, {'color': 'white'}, {'color': 'white'}, {'color': 'white'})

media_bitonto = round(data2.loc[data2.groupby('portale')['data'].idxmax(), ['portale', 'percentuale']][data2.loc[data2.groupby('portale')['data'].idxmax(), ['portale', 'percentuale']]['portale'] == 'Comune di Bitonto']['percentuale'].values[0], 2)
media_crispiano = round(data2.loc[data2.groupby('portale')['data'].idxmax(), ['portale', 'percentuale']][data2.loc[data2.groupby('portale')['data'].idxmax(), ['portale', 'percentuale']]['portale'] == 'Comune di Crispiano']['percentuale'].values[0], 2)
media_massafra = round(data2.loc[data2.groupby('portale')['data'].idxmax(), ['portale', 'percentuale']][data2.loc[data2.groupby('portale')['data'].idxmax(), ['portale', 'percentuale']]['portale'] == 'Comune di Massafra']['percentuale'].values[0], 2)
class Dashboard:
    def __init__(self):
        event_bus.event_bus.subscribe(self)
        app.layout = html.Div([store,
                               html.Link(rel='stylesheet', type='text/css',
                                         href='https://fonts.googleapis.com/css2?family=Source+Sans+Pro&display=swap'),

                               # Sezione Indicatore principale
                               html.Div([
                                   html.Img(src=app.get_asset_url('logo.png'),
                                            style={'height': '170px', 'padding-left': '15px'}),
                                   html.Div([
                                       html.H1(
                                           'Percentuale media della FAIRness degli OpenData dei 3 Comuni Pugliesi',
                                           style={'textAlign': 'left', 'color': 'black','fontFamily': 'Sans Serif',
                                                  'fontSize': '20px'}, id='percentuale-media-div'),
                                       html.H2(
                                           f"{(media_bitonto + media_crispiano + media_massafra) / 3:.2f}%",
                                           style={'textAlign': 'center', 'color': 'black', 'fontSize': '38px',
                                                  'fontFamily': 'Sans Serif',
                                                  'margin-top': '5px', 'margin-bottom': '2px'}),
                                   ], style={'width': '380px', 'height': '140px', 'borderRadius': '15px',
                                             'backgroundColor': '#5EBC67',
                                             'marginLeft': '50px', 'padding-left': '20px'}),
                                   html.Div([
                                       html.H1('Percentuale media del livello di FAIRness degli OpenData del Comune di BITONTO',
                                               style={'textAlign': 'left', 'color': 'black',
                                                      'fontFamily': 'Sans Serif',
                                                      'fontSize': '20px'}),
                                       html.H2(
                                           f"{media_bitonto}%",
                                           style={'textAlign': 'center', 'color': 'black', 'fontSize': '38px',
                                                  'fontFamily': 'Sans Serif',
                                                  'margin-top': '5px', 'margin-bottom': '2px'}),
                                   ], style={'width': '380px', 'height': '140px', 'borderRadius': '15px',
                                             'backgroundColor': '#5EBC67',
                                             'marginLeft': '50px', 'padding-left': '20px'}),
                                   html.Div([
                                       html.H1('Percentuale media del livello di FAIRness degli OpenData del Comune di CRISPIANO',
                                               style={'textAlign': 'left', 'color': 'black',
                                                      'fontFamily': 'Sans Serif',
                                                      'fontSize': '20px'}),
                                       html.H2(
                                           f"{media_crispiano}%",
                                           style={'textAlign': 'center', 'color': 'black', 'fontSize': '38px',
                                                  'fontFamily': 'Sans Serif',
                                                  'margin-top': '5px', 'margin-bottom': '2px'}),
                                   ], style={'width': '380px', 'height': '140px', 'borderRadius': '15px',
                                             'backgroundColor': '#5EBC67',
                                             'marginLeft': '50px', 'padding-left': '20px'}),
                                   html.Div([
                                       html.H1('Percentuale media del livello di FAIRness degli OpenData del Comune di MASSAFRA',
                                               style={'textAlign': 'left', 'color': 'black',
                                                      'fontFamily': 'Sans Serif',
                                                      'fontSize': '20px'}),
                                       html.H2(
                                           f"{media_massafra}%",
                                           style={'textAlign': 'center', 'color': 'black', 'fontSize': '38px',
                                                  'fontFamily': 'Sans Serif',
                                                  'margin-top': '5px', 'margin-bottom': '2px'}),
                                   ], style={'width': '380px', 'height': '140px', 'borderRadius': '15px',
                                             'backgroundColor': '#5EBC67',
                                             'marginLeft': '50px', 'padding-left': '20px'})
                               ], style={'display': 'flex', 'alignItems': 'center', 'margin-top': '25px',
                                         'margin-bottom': '25px'},
                               ),

                               # Sezione per la valutazione
                               html.Div([
                                   html.Div([
                                       html.H2(
                                           'Seleziona uno o più Holder per visualizzarne la valutazione complessiva',
                                           style={'textAlign': 'left', 'color': 'black',
                                                  'fontFamily': 'Sans Serif',
                                                  'fontSize': '19px',
                                                  'margin-top': '-0.1cm'}),
                                       html.H2(
                                           'Seleziona uno o più Dataset per visualizzarne la valutazione più recente',
                                           style={'textAlign': 'left', 'color': 'black',
                                                  'fontFamily': 'Sans Serif',
                                                  'fontSize': '19px',
                                                  'margin-top': '-0.2cm'}),
                                       html.Div([
                                           html.Div([
                                               html.Label(html.Img(src=app.get_asset_url('logo2.png'),
                                                                   style={'width': '100%', 'height': '100%',
                                                                          'object-fit': 'contain'}))],
                                               style={'display': 'inline-block', 'width': '40%', 'padding': '10px'}),
                                           html.Div([
                                               html.H2(text_values['text4'], id='text4',
                                                       style={'textAlign': 'left', 'color': 'black',
                                                              'fontFamily': 'Sans Serif',
                                                              'fontSize': '22px',
                                                              'margin-top': '-0.1cm'}),
                                               html.H2(text_values['text'], id='text',
                                                       style={'textAlign': 'left', 'color': 'black',
                                                              'fontFamily': 'Sans Serif',
                                                              'fontSize': '22px',
                                                              'margin-top': '-0.1cm'}),
                                               html.H2(text_values['text1'], id='text1',
                                                       style={'textAlign': 'left', 'color': 'black',
                                                              'fontFamily': 'Sans Serif',
                                                              'fontSize': '22px',
                                                              'margin-top': '-0.1cm'}),
                                               html.H2(text_values['text2'], id='text2',
                                                       style={'textAlign': 'left', 'color': 'black',
                                                              'fontFamily': 'Sans Serif',
                                                              'fontSize': '22px',
                                                              'margin-top': '-0.1cm'}),
                                               html.H2(text_values['text3'], id='text3',
                                                       style={'textAlign': 'left', 'color': 'black',
                                                              'fontFamily': 'Sans Serif',
                                                              'fontSize': '22px',
                                                              'margin-top': '-0.1cm'}),
                                               dcc.Interval(id='interval-component2', interval=1000, n_intervals=0)
                                           ], style={'display': 'inline-block', 'width': '50%', 'padding': '10px'})
                                       ], style={'display': 'flex', 'backgroundColor': 'white', 'padding': '20px',
                                                 'borderRadius': '15px',
                                                 'margin-bottom': '20px'})
                                   ], style={'width': '50%', 'padding': '10px'}),
                                   html.Div([
                                       dcc.Dropdown(
                                           id='holder-dropdown',
                                           options=[{'label': f'{holder}', 'value': holder} for holder in
                                                    data['holder'].unique()],
                                           multi=True,
                                           value=[]
                                       ),
                                       dcc.Dropdown(
                                           id='dataset_id-dropdown',
                                           options=[{'label': f'{id_}', 'value': id_} for id_ in
                                                    data['dataset_id'].unique()],
                                           multi=True,
                                           value=[]
                                       ),
                                       html.Button('Seleziona Tutti gli Holder', id='select-all-holder-button',
                                                   n_clicks=0),
                                       html.Button('Seleziona Tutti gli ID', id='select-all-id-button', n_clicks=0),
                                       dcc.Graph(id='histogram'),
                                       dcc.Interval(id='interval-component', interval=10000, n_intervals=0),
                                       html.Div(id='valutazione-output')
                                   ], style={'width': '50%', 'padding': '10px'})
                               ], style={'display': 'flex', 'backgroundColor': '#5EBC67', 'padding': '20px',
                                         'borderRadius': '15px',
                                         'margin-bottom': '20px'}),

                               # Sezione per la valutazione dei portali
                               html.Div([
                                   dcc.Dropdown(
                                       id='portale-dropdown',
                                       options=[{'label': f'{comune}', 'value': url} for comune, url in
                                                know_urls.items()],
                                       multi=False,
                                       value=None,
                                       placeholder='Seleziona un portale'
                                   ),
                                   dcc.Dropdown(
                                       id='valutazione-dropdown',
                                       options=[{'label': f' {method}', 'value': idx + 1} for idx, method in
                                                enumerate(deepness_value)],
                                       multi=False,
                                       value=None,
                                       placeholder='Seleziona un tipo di valutazione'
                                   ),
                                   html.Div([
                                       html.Button('Avvia Valutazione', id='avvia-valutazione-button', n_clicks=0,
                                                   style={'margin-right': '10px'}),
                                       html.Button('Valuta tutti', id='valuta-tutti-button', n_clicks=0)
                                   ], style={'margin-top': '10px'}),
                               ], style={'padding': '20px', 'borderRadius': '15px', 'backgroundColor': '#5EBC67'})
                               ])

    def run_dash(self):
        app.run_server(debug=True)

    def handle_event(self, event):
        if event['type'] == 'update_text_toplevel':
            if 'text' in event and event['text']:
                text_values['text'] = event['text']
            if 'text1' in event and event['text1']:
                text_values['text1'] = event['text1']
            if 'text2' in event and event['text2']:
                text_values['text2'] = event['text2']
            if 'text3' in event and event['text3']:
                text_values['text3'] = event['text3']
            if 'text4' in event and event['text4']:
                text_values['text4'] = event['text4']
