from abc import ABC, abstractmethod
from typing import Union
import dataclasses

class MetalakeChange:

    @staticmethod
    def rename(new_name):
        return RenameMetalake(new_name)

    @staticmethod
    def update_comment(new_comment):
        return UpdateMetalakeComment(new_comment)

    @staticmethod
    def set_property(property, value):
        return SetProperty(property, value)

    @staticmethod
    def remove_property(property):
        return RemoveProperty(property)


class RenameMetalake(MetalakeChange):
    def __init__(self, new_name):
        self.new_name = new_name

    def get_new_name(self):
        return self.new_name

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, RenameMetalake):
            return False
        return self.new_name == other.new_name

    def __hash__(self):
        return hash(self.new_name)

    def __str__(self):
        return f"RENAMEMETALAKE {self.new_name}"

class UpdateMetalakeComment(MetalakeChange):
    def __init__(self, new_comment):
        self.new_comment = new_comment

    def get_new_comment(self):
        return self.new_comment

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, UpdateMetalakeComment):
            return False
        return self.new_comment == other.new_comment

    def __hash__(self):
        return hash(self.new_comment)

    def __str__(self):
        return f"UPDATEMETALAKECOMMENT {self.new_comment}"

class SetProperty(MetalakeChange):
    def __init__(self, property_name, value):
        self.property_name = property_name
        self.value = value

    def get_property(self):
        return self.property_name

    def get_value(self):
        return self.value

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, SetProperty):
            return False
        return self.property_name == other.property_name and self.value == other.value

    def __hash__(self):
        return hash((self.property_name, self.value))

    def __str__(self):
        return f"SETPROPERTY {self.property_name} {self.value}"


class RemoveProperty(MetalakeChange):
    def __init__(self, property):
        self.property = property

    def get_property(self):
        return self.property

    def __eq__(self, other):
        if not isinstance(other, RemoveProperty):
            return False
        return self.property == other.property

    def __hash__(self):
        return hash(self.property)

    def __str__(self):
        return f"REMOVEPROPERTY {self.property}"
