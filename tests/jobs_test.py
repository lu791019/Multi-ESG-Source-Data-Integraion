# import unittest
# from unittest.mock import MagicMock
# import pandas as pd

# from jobs import data_check
# from jobs import excel_to_raw
# from jobs import csr_to_raw


# class TestJobsMethods(unittest.TestCase):


#     def test_given_current_date_10_should_data_check_excute(self):

#         current_date = 10
#         self.pd_read_sql = pd.read_sql
#         pd.read_sql = MagicMock()
#         self.data_check_update_data_status = data_check.update_data_status
#         data_check.update_data_status = MagicMock()

#         data_check.data_check(current_date)

#         pd.read_sql.assert_called()


#         pd.read_sql = self.pd_read_sql
#         data_check.update_data_status.reset_mock()
#         data_check.update_data_status = self.data_check_update_data_status

#     def test_given_invalid_excel_should_be_false(self):

#         expect = False
#         result = excel_to_raw.handle_waste(
#             pd.DataFrame(data={
#                 'col1': [1]
#             })
#         )
#         self.pd_read_sql = pd.read_sql
#         pd.read_sql = MagicMock()
#         self.data_check_update_data_status = data_check.update_data_status
#         data_check.update_data_status = MagicMock()

#         self.assertEqual(expect, result)


#         pd.read_sql = self.pd_read_sql
#         data_check.update_data_status.reset_mock()
#         data_check.update_data_status = self.data_check_update_data_status

#     def test_given_valid_excel_should_be_true(self):

#         expect = True
#         result = excel_to_raw.handle_waste(
#             pd.DataFrame(data={
#                 '2021.07': [1]
#             })
#         )
#         self.pd_read_sql = pd.read_sql
#         pd.read_sql = MagicMock()
#         self.data_check_update_data_status = data_check.update_data_status
#         data_check.update_data_status = MagicMock()

#         self.assertEqual(expect, result)

#         pd.read_sql = self.pd_read_sql
#         data_check.update_data_status.reset_mock()
#         data_check.update_data_status = self.data_check_update_data_status

#     def test_given_invalid_indicatorid_should_csr_to_raw_be_false(self):

#         expect = False
#         csr_to_raw.update_csr_data = MagicMock()
#         self.update_csr_data = csr_to_raw.update_csr_data

#         result = csr_to_raw.import_csr_data([99999])

#         self.assertEqual(expect, result)

#         csr_to_raw.update_csr_data = self.update_csr_data

# if __name__ == '__main__':
#     unittest.main()
