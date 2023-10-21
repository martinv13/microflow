class RunStrategy:
    ANY_SUCCESS_NO_ERROR = 1
    ANY_SUCCESS = 2
    ALL_SUCCESS = 3
    ALWAYS = 4

    def __init__(self, runnable, run_strategy):
        self.runnable = runnable
        if run_strategy not in {
            RunStrategy.ANY_SUCCESS_NO_ERROR,
            RunStrategy.ANY_SUCCESS,
            RunStrategy.ALL_SUCCESS,
            RunStrategy.ALWAYS,
        }:
            raise ValueError("unknown run_strategy")
        self.run_strategy = run_strategy

    def should_run(self, *args):
        return True
