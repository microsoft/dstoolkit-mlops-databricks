#%load_ext autoreload #
#%autoreload 2

# COMMAND ----------
from utils import utils_test_function

def run():
    c = utils_test_function()
    print(c)

    return c


dbutils.library.restartPython()

if __name__ == "__main__":
    
    c = run()
    print(c)

# COMMAND ----------