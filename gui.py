import tkinter as tk
from tkinter import messagebox
from PIL import ImageTk, Image

import event_bus

gui_el_path = 'file/gui_elements/'


def box(title, text, _icon='warning'):
    msg_box = tk.messagebox.askquestion(title, text, icon=_icon)
    if msg_box == 'yes':
        ans = 1
    else:
        ans = 0
    return ans


class DatabaseUpdate(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)

        event_bus.event_bus.subscribe(self)

        self.withdraw()
        container = tk.Frame(self, background="white")
        container.grid(row=0, column=0)

        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        self.logo = ImageTk.PhotoImage(Image.open(gui_el_path + "logo.png").resize((1000, 600)))

        label = tk.Label(container)
        label.grid(row=0, column=0)
        label.config(background="white", image=self.logo, wraplength=400)

        self.text = tk.Label(container, wraplength=400)
        self.text.config(background="grey", font=("Inter", 17), text="LOADING..", wraplength=4000)
        self.text.grid(row=2, column=0)

        self.text1 = tk.Label(container, wraplength=400)
        self.text1.config(background="white", font=("Inter", 17), text="loading..", wraplength=4000)
        self.text1.grid(row=3, column=0)

        self.text2 = tk.Label(container, wraplength=400)
        self.text2.config(background="white", font=("Inter", 17), text="loading..", wraplength=4000)
        self.text2.grid(row=4, column=0)

        self.text3 = tk.Label(container, wraplength=400)
        self.text3.config(background="white", font=("Inter", 17), text="loading..", wraplength=4000)
        self.text3.grid(row=5, column=0)

        self.text4 = tk.Label(container, wraplength=400)
        self.text4.config(background="white", font=("Inter", 17), text="loading..", wraplength=4000)
        self.text4.grid(row=1, column=0)

    def handle_event(self, event):
        if event['type'] == 'update_text_toplevel':
            if 'text' in event and event['text']:
                self.text.config(text=event['text'])
            if 'text1' in event and event['text1']:
                self.text1.config(text=event['text1'])
            if 'text2' in event and event['text2']:
                self.text2.config(text=event['text2'])
            if 'text3' in event and event['text3']:
                self.text3.config(text=event['text3'])
        elif event['type'] == 'update_done_toplevel':
            self.destroy()
        elif event['type'] == 'forced_closure':
            self.destroy()
        elif event['type'] == 'portal':
            self.text4.config(text=f'Working on {event["portal"]}')
        elif event['type'] == 'w_show':
            self.deiconify()
        elif event['type'] == 'w_hide':
            self.withdraw()

# from tkinter import filedialog
#
#
# file_path = filedialog.asksaveasfilename(
#     title="Salva il file",
#     filetypes=[("File di testo", "*.txt"), ("Tutti i file", "*.*")]
# )
#
# if file_path:
#     from tkinter import filedialog
#
#     # L'utente ha scelto una posizione per il salvataggio del file
#     with open(file_path, 'w') as file:
#         file.write("Contenuto del file da salvare")