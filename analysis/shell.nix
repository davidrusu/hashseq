{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # Defines a python + set of packages.
    (python3.withPackages (ps: with ps; with python3Packages; [
      jupyter
      ipython

      # Uncomment the following lines to make them available in the shell.
      # pandas
      numpy
      matplotlib
      scipy
      sympy
      pyvis
      networkx
    ]))
  ];

  # Automatically run jupyter when entering the shell.
  shellHook = "jupyter notebook";
}
