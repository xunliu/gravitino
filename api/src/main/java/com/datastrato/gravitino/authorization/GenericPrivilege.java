package com.datastrato.gravitino.authorization;

public abstract class GenericPrivilege<T extends GenericPrivilege<T>> implements Privilege {
  @FunctionalInterface
  public interface GenericPrivilegeFactory<T extends GenericPrivilege<T>> {
    T create(Privilege.Condition condition, Privilege.Name name);
  }

  private final Condition condition;
  private final Name name;

  protected GenericPrivilege(Condition condition, Name name) {
    this.condition = condition;
    this.name = name;
  }

  /** @return The instance with allow condition of the privilege. */
  public static <T extends GenericPrivilege<T>> T allow(
      Name name, GenericPrivilegeFactory<T> factory) {
    return factory.create(Condition.ALLOW, name);
  }

  /** @return The instance with deny condition of the privilege. */
  public static <T extends GenericPrivilege<T>> T deny(
      Name name, GenericPrivilegeFactory<T> factory) {
    return factory.create(Condition.DENY, name);
  }

  /** @return The generic name of the privilege. */
  @Override
  public Name name() {
    return name;
  }

  /** @return The condition of the privilege. */
  @Override
  public Condition condition() {
    return condition;
  }

  /** @return A readable string representation for the privilege. */
  @Override
  public String simpleString() {
    return condition.name() + " " + name.name().toLowerCase().replace('_', ' ');
  }
}
