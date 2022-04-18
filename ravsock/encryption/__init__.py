def get_context():
    import tenseal as ts

    # Setup TenSEAL context
    context = ts.context(
        ts.SCHEME_TYPE.CKKS,
        poly_modulus_degree=8192,
        coeff_mod_bit_sizes=[60, 40, 40, 60],
    )

    context.generate_galois_keys()
    context.global_scale = 2 ** 40
    return context


def dump_context(context, filepath, save_secret_key=False):
    with open(filepath, "wb") as f:
        f.write(context.serialize(save_secret_key=save_secret_key))


def load_context(file_path):
    import tenseal as ts

    with open(file_path, "rb") as f:
        return ts.context_from(f.read())
