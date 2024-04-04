import unittest
from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin
from typing import Optional

from gravitino.dto.metalake_dto import MetalakeDTO


@dataclass
class BaseResponse(DataClassJsonMixin):
    code: int

    # def __init__(self, code: int):
    #     self.code = code

    @classmethod
    def default(cls):
        return cls(code=0)

    def validate(self):
        if self.code < 0:
            raise ValueError("code must be >= 0")

@dataclass
class MetalakeResponse(BaseResponse):
    metalake: Optional[MetalakeDTO]

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)  # Pass any arguments to the parent class
    #     self.metalake = kwargs.get('metalake', None)  # Explicitly handle 'metalake'

    def validate(self):
        super().validate()

    def test_from_json_metalake_response(self, *args):
        str = b'{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment","properties":{"key1":"value1","key2":"value2"},"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        gravitinoMetalake = MetalakeResponse.from_json(str)

# # 定义 MetalakeDTO 类
# @dataclass
# class MetalakeDTO:
#     name: str
#     comment: str
#     properties: dict
#     audit: dict

class TestGravitinoClient(unittest.TestCase):
    def test_from_json_metalake_response(self):
        json_str = '{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment","properties":{"key1":"value1","key2":"value2"},"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        metalake_response = MetalakeResponse.from_json(json_str)
        self.assertEqual(metalake_response.code, 0)
        self.assertIsNotNone(metalake_response.metalake)
        self.assertEqual(metalake_response.metalake.name, "example_name18")

    def test_from_json_metalake_response(self, *args):
        # 测试 MetalakeResponse 类的 from_json 方法
        MetalakeResponse.test_from_json_metalake_response(self)


if __name__ == '__main__':
    unittest.main()