import cmd
from app.weather import collect_weather_display, health, store_weather_data, query_all, collect_batch
import os

class my_shell(cmd.Cmd):
    intro = "Welcome to The Shell.\n"
    prompt = "(shell)"

    def preloop(self):
        self.do_help("")

    def do_quit(self, line):
        "Exit the shell"
        print("Goodbye.")
        return True
    
    def do_status(self, line):
        "Check the status of database"
        health()

    def do_collect(self, line):
        "Collect current weather for Philadelphia"
        collect_weather_display()

    def do_collect_store(self, line):
        "Collect current weather for Philadelphia and store it in PostgreSQL Database"
        data = collect_weather_display()
        if data:
            store_weather_data(data)
        
    def do_clear(self, line):
        "Clear the terminal screen and reintialize shell"
        os.system('clear')
        self.do_help("")

    def do_query(self, line):
        "Show all entries in the weather_db table"
        query_all()
    
    def do_collect_batch(self, line):
        "Collect weather data for all the cities in 'cities.csv'"
        collect_batch()


if __name__ == "__main__":
    my_shell().cmdloop()