CODE_DIR      = ./
PYTHON_CODE   = $(CODE_DIR)src/
PYTHON_TESTS  = $(CODE_DIR)src/tests/
PYTHONPATH         = $(subst C:/,/c/,$(realpath $(PYTHON_CODE)))
PYTHONPATH_TESTS   = $(PYTHONPATH):$(subst C:/,/c/,$(realpath $(PYTHON_TESTS)))

ifneq ($(findstring Win,${OS})$(findstring MINGW,${OS}),)
	# This variable should only be set on Windows
	OS = windows
	# Python 3 binary on Windows is only called python.exe
	PYTHON ?= python
	FIND = "C:\\Program Files\\Git\\usr\\bin\\find.exe"
else
	OS = $(shell uname -s | tr '[:upper:]' '[:lower:]')
	PYTHON ?= python3
	FIND = find
endif

.PHONY: test

test:
	PYTHONPATH=$(PYTHONPATH_TESTS) $(PYTHON) -m pytest $(PYTHON_TESTS)
