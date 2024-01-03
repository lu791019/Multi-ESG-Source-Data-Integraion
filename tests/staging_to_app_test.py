import unittest
from unittest.mock import MagicMock
import pandas as pd

# from jobs import staging_to。_app

# class TestJobsMethods(unittest.TestCase):
#     def setUp(self) -> None:
#         self.staging_to_app = staging_to_app
#         self.staging_to_app.db_operate = MagicMock(return_value=True)

#     def tearDown(self) -> None:
#         self.staging_to_app = staging_to_app

#     def test_should_staging_to_app_reporting_summary_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('reporting_summary')

#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_energy_overview_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('energy_overview')


#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_carbon_emission_overview_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('carbon_emission_overview')

#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_renewable_energy_overview_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('renewable_energy_overview')

#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_electricity_overview_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('electricity_overview')

#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_water_overview_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('water_overview')

#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_electricity_unit_overview_be_true(self):
#         expect = True

#         result = self.staging_to_app.staging_to_app('electricity_unit_overview')

#         self.assertEqual(expect, result)


#     def test_should_staging_to_app_waste_overview_be_true(self):
#         staging_to_app.is_data_exist = MagicMock(return_value=True)
#         staging_to_app.waste_operate = MagicMock(return_value=True)

#         self.staging_to_app.staging_to_app('waste_overview')

#         staging_to_app.waste_operate.called

#     def test_when_data_empty_should_staging_to_app_waste_overview_be_false(self):
#         expect = False

#         staging_to_app.is_data_exist = MagicMock(return_value=False)
#         result = self.staging_to_app.staging_to_app('waste_overview')

#         self.assertEqual(expect, result)

# if __name__ == '__main__':
#     unittest.main()
