import importlib.util
import sys
import types
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "data_quality" / "dq.lambda.py"


class DqLambdaConfigTests(unittest.TestCase):
    def load_module(self):
        boto3_module = types.ModuleType("boto3")
        boto3_module.client = lambda *args, **kwargs: object()
        sys.modules["boto3"] = boto3_module

        awswrangler_module = types.ModuleType("awswrangler")
        awswrangler_module.athena = types.SimpleNamespace(read_sql_query=lambda *args, **kwargs: None)
        sys.modules["awswrangler"] = awswrangler_module

        pandas_module = types.ModuleType("pandas")
        pandas_module.DataFrame = object
        pandas_module.to_datetime = lambda *args, **kwargs: None
        sys.modules["pandas"] = pandas_module

        spec = importlib.util.spec_from_file_location("dq_lambda_under_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_resolve_database_prefers_event_value(self):
        module = self.load_module()
        self.assertEqual(module.resolve_database({"database": "event-db"}, {}), "event-db")

    def test_resolve_database_uses_environment_override(self):
        module = self.load_module()
        self.assertEqual(module.resolve_database({}, {"GLUE_DB_SILVER": "env-db"}), "env-db")


if __name__ == "__main__":
    unittest.main()
