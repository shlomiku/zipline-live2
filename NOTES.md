# Zipline notes
## Execution flow
 * zipline run -f ..*:
 * __main__.py / run()
 * run_algo.py / _run()
   * Creates TradingAlgorithm
   * Returns TradingAlgorithm.run()'s result
 * algorithm.py / TradingAlgorithm.run()
   * iterates over algorithm generators through get_generator -> create_generator()
 * algorithm.py / TradingAlgorithm._create_generator()
   * creates trading_client out of AlgorithmSimulator or AlgorithmLiveExecutor
   * returns with trading_client.transform()
 * AlgorithmSimulator.transform()
   * yields 'perf messages':
     * capital_change
     * daily_perf or minute_perf
     * RiskReport as dict()
   * every yielded perf message is appended to perfs in TradingAlgorithm.run()
 * algorithm / TradingAlgorithm.run()
   * daily_stats calculated
   * analyzed

## Bar by bar processing:
 * trading_client.transform() iterates over clock
 * for each tick every_bar() is called
   * calls series of handle_data (events.py, algorithm.py & script)
 * in script's handle_data() the user can call
   * data.current -> BarData.current() which calls
     * data_portal.get_spot_value()
     * data_portal.get_adjusted_value()
 * data_portal.get_spot_value() / data_portal.get_adjusted value() calls BarData.simulation_dt_func()
     * both functions call BarData.simulation_dt_func() which translates (through trading_client.get_simulation_dt()) to trading_client.every_bar()'s dt_to_use timestamp


 # Ticks
[2017-05-07 16:42:35.531282] INFO: Trade Simulation: Processing tick: 2017-04-20 00:00:00+00:00 - 1 (SESSION_START)
[2017-05-07 16:42:35.531622] INFO: Trade Simulation: Processing tick: 2017-04-20 12:45:00+00:00 - 4 (BEFORE_TRADING_START)
[2017-05-07 16:42:35.532042] INFO: Trade Simulation: Processing tick: 2017-04-20 13:31:00+00:00 - 0 (BAR)
[2017-05-07 16:42:35.532133] INFO: Trade Simulation: Processing tick: 2017-04-20 13:32:00+00:00 - 0
...
[2017-05-07 16:42:35.555722] INFO: Trade Simulation: Processing tick: 2017-04-20 19:59:00+00:00 - 0
[2017-05-07 16:42:35.555768] INFO: Trade Simulation: Processing tick: 2017-04-20 20:00:00+00:00 - 0
[2017-05-07 16:42:35.555878] INFO: Trade Simulation: Processing tick: 2017-04-20 20:00:00+00:00 - 2 (SESSION_END)
[2017-05-07 16:42:35.555946] INFO: Trade Simulation: Processing tick: 2017-04-21 00:00:00+00:00 - 1
[2017-05-07 16:42:35.556109] INFO: Trade Simulation: Processing tick: 2017-04-21 12:45:00+00:00 - 4
[2017-05-07 16:42:35.556358] INFO: Trade Simulation: Processing tick: 2017-04-21 13:31:00+00:00 - 0
