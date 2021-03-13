#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject* hashc_hello(PyObject *self, PyObject *args) {
    const char *msg;
    if (!PyArg_ParseTuple(args, "s", &msg)) {
        return NULL;
    }
    return PyLong_FromLong(strlen(msg));
}

static PyMethodDef methods[] = {
    {"hello", hashc_hello, METH_VARARGS, "Sample function."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef hashcmodule = {
    PyModuleDef_HEAD_INIT,
    "hashc",
    NULL,
    -1,
    methods
};

PyMODINIT_FUNC PyInit_hashc(void) {
    return PyModule_Create(&hashcmodule);
}
