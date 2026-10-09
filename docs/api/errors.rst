Errors
======

Every error kbus raises derives from :class:`kbus.Error`. The table
Failures on the :doc:`overview </index>` says which situation gives which
error.

.. autoexception:: kbus.Error

.. autoexception:: kbus.RemoteError

.. autoexception:: kbus.NoSuchMember

.. autoexception:: kbus.NoSuchRoute

.. autoexception:: kbus.NoSuchInstance

.. autoexception:: kbus.Refused

.. autoexception:: kbus.Rejected

.. autoexception:: kbus.LinkLost

.. autoexception:: kbus.Unreachable

.. autoexception:: kbus.Aborted

.. autoexception:: kbus.FrameTooLarge

.. autoexception:: kbus.ProtocolError

.. autoexception:: kbus.Overloaded
