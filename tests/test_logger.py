#!/usr/bin/env python3

import unittest
import sys
import os
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.context import ExperimentInformation   # noqa
from simulator.context import ExperimentStatus   # noqa
from simulator.context import Context  # noqa
from simulator.context import DistributedContext  # noqa
from simulator.tasks import Task, TaskBundle  # noqa
from simulator.resources import Resource  # noqa
from simulator.logger import Logger  # noqa


class LoggerTest(unittest.TestCase):
    def setUp(self):
        self.logger = Logger(0, 1, 'test')

    def test_attributes(self):
        self.assertEqual(self.logger.screen_verbosity, 0)
        self.assertEqual(self.logger.logging_verbosity, 1)
        self.assertEqual(self.logger.file_prefix, 'test')
        self.assertEqual(self.logger.num_migrations, 0)
        self.assertEqual(self.logger.num_round, 0)
        self.assertEqual(self.logger.num_load_checks, 0)

    def test_register_start_and_end(self):
        info = ExperimentInformation()
        self.logger.register_start(info)
        self.logger.register_end()

        expected_log = ('\n-- Start of experiment --\n' +
                        '- Running scheduler None for 0 tasks and' +
                        ' 0 resources.\n- [RNG seed (0);' +
                        ' Bundle load limit (10); Epsilon (1.05)]\n' +
                        '-- End of experiment --\n')
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(expected_log, written_log)
        os.remove('test_log.txt')

        expected_stats = ('# tasks:0 resources:0 rng_seed:0' +
                          ' algorithm:None\n' +
                          'round,maxload,overloaded,underloaded,' +
                          'avgloaded,migration_count,load_checks\n')
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(expected_stats, written_stats)
        os.remove('test_stats.csv')

    def test_register_resource_status(self):
        status = ExperimentStatus(1, 2, 3, 4)
        self.logger.register_resource_status(status)
        self.logger.register_end()

        expected_log = ('[0] -- Status:\n' +
                        '- maximum load is 1\n' +
                        '- number of overloaded, underloaded and average' +
                        ' loaded resources are 2, 3, and 4\n' +
                        '- number of migrations is 0\n' +
                        '- number of load checks is 0\n' +
                        '-- End of experiment --\n')
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(expected_log, written_log)
        os.remove('test_log.txt')

        expected_stats = '0,1,2,3,4,0,0\n'
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(expected_stats, written_stats)
        os.remove('test_stats.csv')

    def test_register_migration(self):
        self.logger.logging_verbosity = 2
        self.logger.register_migration(1, 2, 3, 4)
        self.assertEqual(self.logger.num_migrations, 1)
        self.logger.register_end()
        os.remove('test_stats.csv')

        expected_log = ('[0] - Task 1 (load 2) migrating from 3 to 4.\n' +
                        '-- End of experiment --\n')
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(expected_log, written_log)
        os.remove('test_log.txt')

    def test_register_new_round(self):
        self.logger.register_new_round()
        self.assertEqual(self.logger.num_round, 1)
        self.assertEqual(self.logger.num_migrations, 0)
        self.assertEqual(self.logger.num_load_checks, 0)
        self.logger.register_end()
        os.remove('test_stats.csv')

        expected_log = ('-- Start of round 1 --\n' +
                        '-- End of experiment --\n')
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(expected_log, written_log)
        os.remove('test_log.txt')

    def test_register_convergence(self):
        self.logger.register_convergence(True, 11, 12)
        self.logger.register_end()
        os.remove('test_stats.csv')

        expected_log = ('[0] -- Convergence check: 11 <= 12 : True\n' +
                        '-- End of experiment --\n')
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(expected_log, written_log)
        os.remove('test_log.txt')

    def test_register_load_check(self):
        self.logger.logging_verbosity = 2
        self.logger.register_load_check(10, 20, 30, 40)
        self.assertEqual(self.logger.num_load_checks, 1)
        self.logger.register_end()
        os.remove('test_stats.csv')

        expected_log = ('[0] - Resource 10 (load 20) requesting' +
                        ' information from resource 30 (load 40)\n'
                        '-- End of experiment --\n')
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(expected_log, written_log)
        os.remove('test_log.txt')


class LoggerFromContextTest(unittest.TestCase):
    def setUp(self):
        self.context = Context.from_csv('test_inputs/01234_input.csv')
        self.context.set_verbosity(0, 2, 'test')

        self.start_log = ('\n-- Start of experiment --\n' +
                          '- Running scheduler none for 5 tasks and' +
                          ' 3 resources.\n- [RNG seed (0);' +
                          ' Bundle load limit (10); Epsilon (1.05)]\n' +
                          '[0] -- Status:\n' +
                          '- maximum load is 8.0\n' +
                          '- number of overloaded, underloaded and average' +
                          ' loaded resources are 1, 2, and 0\n' +
                          '- number of migrations is 0\n' +
                          '- number of load checks is 0\n')
        self.end_log = '-- End of experiment --\n'
        self.start_status = ('# tasks:5 resources:3 rng_seed:0' +
                             ' algorithm:none\n' +
                             'round,maxload,overloaded,underloaded,' +
                             'avgloaded,migration_count,load_checks\n' +
                             '0,8.0,1,2,0,0,0\n')

    def test_log_finish(self):
        self.context.log_finish()

        middle_log = ('[1] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

        middle_status = '1,8.0,1,2,0,0,0\n'
        status = self.start_status + middle_status
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(status, written_stats)
        os.remove('test_stats.csv')

    def test_update_mapping(self):
        self.context.update_mapping(0, 1)
        self.context.log_finish()

        middle_log = ('[0] - Task 0 (load 1.0) migrating from 2 to 1.\n' +
                      '[1] -- Status:\n' +
                      '- maximum load is 9.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 1\n' +
                      '- number of load checks is 0\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

        middle_status = '1,9.0,1,2,0,1,0\n'
        status = self.start_status + middle_status
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(status, written_stats)
        os.remove('test_stats.csv')


class LoggerFromDistributedContextTest(unittest.TestCase):
    def setUp(self):
        self.context = DistributedContext.from_csv(
            'test_inputs/01234_input.csv')
        self.context.set_verbosity(0, 2, 'test')

        self.start_log = ('\n-- Start of experiment --\n' +
                          '- Running scheduler none for 5 tasks and' +
                          ' 3 resources.\n- [RNG seed (0);' +
                          ' Bundle load limit (10); Epsilon (1.05)]\n')
        self.end_log = '-- End of experiment --\n'
        self.start_status = ('# tasks:5 resources:3 rng_seed:0' +
                             ' algorithm:none\n' +
                             'round,maxload,overloaded,underloaded,' +
                             'avgloaded,migration_count,load_checks\n')

    def test_has_converged(self):
        self.context.has_converged()
        self.context.log_finish()
        os.remove('test_stats.csv')

        middle_log = ('[0] -- Convergence check: 8.0 <= 5.25 : False\n' +
                      '[0] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

    def test_prepare_round(self):
        self.context.prepare_round()
        self.context.log_finish()

        middle_log = ('[0] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n' +
                      '-- Start of round 1 --\n' +
                      '[1] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

        middle_status = '0,8.0,1,2,0,0,0\n1,8.0,1,2,0,0,0\n'
        status = self.start_status + middle_status
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(status, written_stats)
        os.remove('test_stats.csv')

    def test_check_viability(self):
        self.context.prepare_round()
        self.context.check_viability(1, 2)
        self.context.log_finish()

        middle_log = ('[0] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n' +
                      '-- Start of round 1 --\n' +
                      '[1] - Resource 1 (load 8.0) requesting' +
                      ' information from resource 2 (load 3.0)\n' +
                      '[1] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 1\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

        middle_status = '0,8.0,1,2,0,0,0\n1,8.0,1,2,0,0,1\n'
        status = self.start_status + middle_status
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(status, written_stats)
        os.remove('test_stats.csv')

    def test_prepare_round_bundled(self):
        self.context.prepare_round_bundled()
        self.context.log_finish()

        middle_log = ('[0] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n' +
                      '-- Start of round 1 --\n' +
                      '[1] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

        middle_status = '0,8.0,1,2,0,0,0\n1,8.0,1,2,0,0,0\n'
        status = self.start_status + middle_status
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(status, written_stats)
        os.remove('test_stats.csv')

    def test_log_finish(self):
        self.context.log_finish()

        middle_log = ('[0] -- Status:\n' +
                      '- maximum load is 8.0\n' +
                      '- number of overloaded, underloaded and average' +
                      ' loaded resources are 1, 2, and 0\n' +
                      '- number of migrations is 0\n' +
                      '- number of load checks is 0\n')
        log = self.start_log + middle_log + self.end_log
        with open('test_log.txt', 'r') as logfile:
            written_log = logfile.read()
            self.assertEqual(log, written_log)
        os.remove('test_log.txt')

        middle_status = '0,8.0,1,2,0,0,0\n'
        status = self.start_status + middle_status
        with open('test_stats.csv', 'r') as statsfile:
            written_stats = statsfile.read()
            self.assertEqual(status, written_stats)
        os.remove('test_stats.csv')


if __name__ == '__main__':
    unittest.main()
